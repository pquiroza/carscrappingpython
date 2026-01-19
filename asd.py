   for r in versiones_formato:
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [r['precio_lista']-r['bono_financiamiento'],r['precio_lista']-r['bono_financiamiento'],r['precio_lista'],r['precio_lista']]
        datos = {
            'marca': r['brand'],
            'model': r['model'],
            'modelDetail': r['version'],
            'tiposprecio': tiposprecio,
            'precio': precio
            
        }
        print(datos)
        print("-"*50)
        