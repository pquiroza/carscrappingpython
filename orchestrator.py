import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse
try:
    import yaml  # pip install pyyaml
except ImportError:
    print("Falta pyyaml. Instala con: pip install pyyaml")
    raise


BASE_DIR = Path(__file__).resolve().parent
RUNS_DIR = BASE_DIR / "runs"
LOGS_DIR = RUNS_DIR / "logs"
STATE_FILE = RUNS_DIR / "state.json"

RUNS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Job:
    id: str
    cmd: List[str]
    timeout_sec: int = 1800
    retries: int = 0


@dataclass
class JobResult:
    job_id: str
    status: str  # queued|running|success|failed|timeout|killed
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    attempt: int = 0
    return_code: Optional[int] = None
    error: Optional[str] = None
    log_file: Optional[str] = None


class Orchestrator:
    def __init__(self, jobs: List[Job], concurrency: int = 2):
        self.jobs = jobs
        self.concurrency = max(1, concurrency)
        self._stop = asyncio.Event()
        self._running_procs: Dict[str, asyncio.subprocess.Process] = {}

    def request_stop(self):
        self._stop.set()

    async def _write_state(self, state: Dict[str, Any]) -> None:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False))
        tmp.replace(STATE_FILE)

    def _load_state(self) -> Dict[str, Any]:
        if STATE_FILE.exists():
            try:
                return json.loads(STATE_FILE.read_text())
            except Exception:
                return {}
        return {}

    async def _run_one(self, job: Job, attempt: int) -> JobResult:
        ts = time.strftime("%Y%m%d-%H%M%S")
        log_path = LOGS_DIR / f"{job.id}-{ts}-attempt{attempt}.log"

        res = JobResult(
            job_id=job.id,
            status="running",
            started_at=time.time(),
            attempt=attempt,
            log_file=str(log_path),
        )

        # persist: running
        state = self._load_state()
        state[job.id] = asdict(res)
        await self._write_state(state)

        try:
            with log_path.open("w", encoding="utf-8") as lf:
                lf.write(f"[START] job={job.id} attempt={attempt}\ncmd={' '.join(job.cmd)}\n\n")
                lf.flush()

                proc = await asyncio.create_subprocess_exec(
                    *job.cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    cwd=str(BASE_DIR),
                    env={**os.environ},
                )
                self._running_procs[job.id] = proc

                async def stream_output():
                    assert proc.stdout is not None
                    async for line in proc.stdout:
                        text = line.decode(errors="replace")
                        lf.write(text)
                        lf.flush()

                stream_task = asyncio.create_task(stream_output())

                try:
                    await asyncio.wait_for(proc.wait(), timeout=job.timeout_sec)
                except asyncio.TimeoutError:
                    res.status = "timeout"
                    res.error = f"Timeout after {job.timeout_sec}s"
                    proc.kill()
                    await proc.wait()
                finally:
                    await stream_task

                res.return_code = proc.returncode
                if res.status == "running":
                    res.status = "success" if proc.returncode == 0 else "failed"

        except asyncio.CancelledError:
            res.status = "killed"
            res.error = "Cancelled"
        except Exception as e:
            res.status = "failed"
            res.error = repr(e)
        finally:
            self._running_procs.pop(job.id, None)
            res.finished_at = time.time()

            # persist final
            state = self._load_state()
            state[job.id] = asdict(res)
            await self._write_state(state)

        return res

    async def _worker(self, queue: "asyncio.Queue[Job]") -> None:
        while not self._stop.is_set():
            try:
                job = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if queue.empty():
                    return
                continue

            # retries loop
            last_res: Optional[JobResult] = None
            for attempt in range(1, job.retries + 2):
                if self._stop.is_set():
                    break
                last_res = await self._run_one(job, attempt)
                if last_res.status == "success":
                    break

            queue.task_done()

    async def run(self) -> Dict[str, JobResult]:
        # initial queue/state
        state = self._load_state()
        for j in self.jobs:
            if j.id not in state:
                state[j.id] = asdict(JobResult(job_id=j.id, status="queued"))
        await self._write_state(state)

        q: asyncio.Queue[Job] = asyncio.Queue()
        for j in self.jobs:
            await q.put(j)

        workers = [asyncio.create_task(self._worker(q)) for _ in range(self.concurrency)]

        await q.join()
        self._stop.set()
        await asyncio.gather(*workers, return_exceptions=True)

        final = self._load_state()
        return {k: JobResult(**v) for k, v in final.items()}

    async def stop_all(self) -> None:
        self.request_stop()
        for job_id, proc in list(self._running_procs.items()):
            try:
                proc.terminate()
            except Exception:
                pass
        await asyncio.sleep(1)
        for job_id, proc in list(self._running_procs.items()):
            if proc.returncode is None:
                try:
                    proc.kill()
                except Exception:
                    pass


def load_jobs_from_yaml(path: Path) -> List[Job]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    jobs = []
    for j in data.get("jobs", []):
        jobs.append(Job(
            id=j["id"],
            cmd=j["cmd"],
            timeout_sec=int(j.get("timeout_sec", 1800)),
            retries=int(j.get("retries", 0)),
        ))
    return jobs


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="IDs separados por coma (ej: kia,volvo)")
    parser.add_argument("--skip", help="IDs separados por coma (ej: mazda,bmw)")
    parser.add_argument("--only-failed", action="store_true", help="Solo failed/timeout del state.json")
    parser.add_argument("--concurrency", type=int, default=int(os.environ.get("ORCH_CONCURRENCY", "2")))
    args = parser.parse_args()

    jobs_file = BASE_DIR / "jobs.yaml"
    if not jobs_file.exists():
        print(f"No existe {jobs_file}.")
        sys.exit(1)

    jobs = load_jobs_from_yaml(jobs_file)

    def parse_csv(value):
        if not value:
            return set()
        return {x.strip() for x in value.split(",") if x.strip()}

    only = parse_csv(args.only)
    skip = parse_csv(args.skip)

    # DEBUG: muestra qué recibió y qué IDs existen
    print(f"[ARGS] only={sorted(list(only))} skip={sorted(list(skip))} only_failed={args.only_failed}")
    print(f"[JOBS] disponibles={ [j.id for j in jobs] }")

    # aplica filtros
    if only:
        jobs = [j for j in jobs if j.id in only]
    if skip:
        jobs = [j for j in jobs if j.id not in skip]

    if args.only_failed:
        state = {}
        if STATE_FILE.exists():
            try:
                state = json.loads(STATE_FILE.read_text())
            except Exception:
                state = {}
        failed_ids = {jid for jid, r in state.items() if isinstance(r, dict) and r.get("status") in {"failed", "timeout"}}
        jobs = [j for j in jobs if j.id in failed_ids]

    print(f"[JOBS] a ejecutar={ [j.id for j in jobs] }")

    if not jobs:
        print("No hay jobs para ejecutar con esos filtros.")
        return

    orch = Orchestrator(jobs, concurrency=args.concurrency)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, orch.request_stop)
        except NotImplementedError:
            pass

    results = await orch.run()

    print("\n=== RESUMEN ===")
    for job_id, r in results.items():
        dur = None
        if r.started_at and r.finished_at:
            dur = round(r.finished_at - r.started_at, 1)
        print(f"- {job_id}: {r.status} (attempt {r.attempt}) rc={r.return_code} dur={dur}s log={r.log_file}")
if __name__ == "__main__":
    asyncio.run(main())
