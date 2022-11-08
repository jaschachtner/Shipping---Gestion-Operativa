from threading import Thread
import requests
import time
import os.path

# Import writer class from csv module
from csv import DictWriter

# Declarations
ERRORS=[]
OUTPUT=[]
URI_BASE = 'https://6f008c57-99e0-4a2e-8d80-782a71cf99db.mock.pstmn.io/'

INPUT_FILE = 'input.txt'    #Ruta Archivo de entrada
OUTPUT_FILE = 'output.csv'  #Ruta Archivo de salida
ERROR_FILE = 'error.txt'    #Ruta Archivo de errores
limit_threads = 2           #Limitador de Hilos

def get_order(order_id : int):

    #Paso a enteros, tengo problemas con caracteres especiales (Corregir)
    order_id = int(order_id)

    #Armo url de ordenes
    url = f"{URI_BASE}orders/{order_id}"

    response = requests.get(url)

    if(response.status_code == 200):
        order = response.json()

        if(order["id"] == order_id):

            return order

        else:

            return

def get_shipping(shipping_id : int):

    #Armo url de envios
    url = f"{URI_BASE}shipments/{shipping_id}"

    response = requests.get(url)

    if(response.status_code == 200):
        shipping = response.json()

        if(shipping["id"] == shipping_id):

            return shipping

        else:

            return      

def write_csv(output):

  # Abro archivo en modo W
  f_object = open(OUTPUT_FILE, 'w')

  # Obtengo cabecera del CSV
  field_names = output[1].keys()

  # Paso el Objeto f_object y el nombre de las columnas
  dictwriter_object = DictWriter(f_object, fieldnames=field_names)

  # Imprimo cabecera
  dictwriter_object.writeheader()
  print(f"Escribiendo en: {OUTPUT_FILE}")    

  # Recorro output appendeando todas las lineas en el CSV
  for line in output:
    
    # Appendeo lineas al CSV
    dictwriter_object.writerow(line)

  # Cierro el fichero
  f_object.close()
  print(f"{OUTPUT_FILE} finalizado.")    

def write_errors(errors):

  with open(ERROR_FILE, 'w') as f:
     f.write('\n'.join(errors)) 
    

def get_request_parallel():

    request_threads = {}
    ouput_threads = {}

    #Abro archivo y ejecuto linea por linea
    
    with open(INPUT_FILE) as f:
      for line in f:

        #Paso a enteros, tengo problemas con caracteres especiales (Corregir)
        order_id = int(''.join(filter(str.isalnum, line)))

        if len(request_threads) >= limit_threads:

            check_request_threads = True

            while check_request_threads:
                request_threads = clean_old_request_threads(request_threads)
                if len(request_threads) >= limit_threads:
                    time.sleep(1)
                else:
                    check_request_threads = False
        
        # REQUEST PARALELOS
        request_threads[order_id] = Thread(target=resolve_line, kwargs={'order_id':order_id})
        request_threads[order_id].start()

        # # CSV Hilo
        # output_threads[order_id] = Thread(target=resolve_line, kwargs={'order_id':order_id})
        # output_threads[order_id].start()


    while(True):
        request_threads = clean_old_request_threads(request_threads)

        if len(request_threads) < 1:
            break

def clean_old_request_threads(request_threads):

    for key, value in request_threads.copy().items():
        if not value.is_alive():
            del request_threads[key]

    return request_threads        


def resolve_line(order_id : int):
    #Obtengo la orden
    order = get_order(order_id)

    #Si no es valida voy al sig. Order_ID
    if order is None:
      msg = f"Orden erronea:{order_id}"
      ERRORS.append(msg)
      print(msg)
      return
    else:
      msg = f"Orden Ok:{order_id}"
      print(msg)

    #Obtengo el envio
    shipping = get_shipping(order['shipping']['id'])

    #Armo registro CSV.
    if shipping['receiver_address']['agency'] is None:
      destination = "Domicilio"
      receiver_address = f"{shipping.get('receiver_address','').get('country','').get('name','')}, {shipping.get('receiver_address','').get('city','').get('name','')}, {shipping.get('receiver_address','').get('address_line','')}, {shipping.get('receiver_address','').get('zip_code','')}"
    else:
      destination = "Agencia"
      receiver_address = f"{shipping.get('receiver_address','').get('agency','').get('agency_id','')}, {shipping.get('receiver_address','').get('agency','').get('carrier_id','')}"

    linea = {
                "ID_orden":order['id'],
                "ID_item":"",
                "description":'',
                "ID_shipment":shipping.get('id',''),
                "status":shipping.get('status',''),
                "substatus":shipping.get('substatus',''),
                "logistic_type": shipping.get('logistic_type',''),
                "destination": destination,
                "receiver address": receiver_address
            }

    #Itero los items
    for item in order['order_items']:
      
      #Actualizo el Item ID
      linea.update({'ID_item': item['item']['id']})
      
      #Actualizo la variacion
      variation = '/'.join(map(lambda var: f"{var['name']} {var['value_name']}", item['item']['variation_attributes']))
      linea.update({'description': f"{item['item']['title']} {variation}"})

      OUTPUT.append(linea)
  

if __name__ == '__main__':
    init = time.time()

    get_request_parallel()

    write_csv(OUTPUT)
    write_errors(ERRORS)
            
    # print('Time ' + str(time.time() - init))
