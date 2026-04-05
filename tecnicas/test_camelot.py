import camelot

tables = camelot.read_pdf("out_nissan_brand/pdfs/0d1d3607af_FICHA_20TECNICA_20NUEVA_20NISSAN_20PATHFINDER.pdf", pages="1", flavor="lattice")
print("tables:", tables.n)
if tables.n:
    print(tables[0].df.head())
