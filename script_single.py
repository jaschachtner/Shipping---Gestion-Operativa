import requests
import time
import os.path

# Import writer class from csv module
from csv import DictWriter

# Declarations
ERRORS=[]
URI_BASE = 'https://6f008c57-99e0-4a2e-8d80-782a71cf99db.mock.pstmn.io/'
OUTPUT_FILE = 'output.csv'

def get_order(order_id : int):

    #Paso a enteros, tengo problemas con caracteres especiales (Corregir)
    order_id = int(''.join(filter(str.isalnum, order_id)))

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

def write_csv(line : dict):
  header = True

  # list of column names
  field_names = line.keys()
 
  # Si el archivo existe, no requiere mostrar cabecera. 
  if os.path.isfile(OUTPUT_FILE) == True:
    header = False

 
  # Open CSV file in append mode
  # Create a file object for this file
  with open(OUTPUT_FILE, 'a') as f_object:
 
    # Pass the file object and a list
    # of column names to DictWriter()
    # You will get a object of DictWriter
    dictwriter_object = DictWriter(f_object, fieldnames=field_names)

    # Imprimo o no, cabecera
    if header == True:
      dictwriter_object.writeheader()
 
    # Pass the dictionary as an argument to the Writerow()
    dictwriter_object.writerow(line)
 
    # Close the file object
    f_object.close()       


if __name__ == '__main__':
    init = time.time()
    
    #Abro archivo y ejecuto linea por linea
    with open('input.txt') as f:
      for line in f:

        #Obtengo la orden
        order = get_order(line)

        #Si no es valida voy al sig. Order_ID
        if order is None:
          ERRORS.append(f"Orden erronea:{line}")
          continue

        #Obtengo el envio
        shipping = get_shipping(order['shipping']['id'])
        print(f"Orden:{order}")
        print(f"Envio:{shipping}")

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
          
          #Appendeo el CSV
          write_csv(linea)
        


    # print('Time ' + str(time.time() - init))
