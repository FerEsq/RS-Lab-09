"""
 * Nombre: kafkaConsumer.py
 * Autores:
    - Fernanda Esquivel, 21542
    - Melissa Pérez, 21385
 * Descripción: Kafka Consumer que recibe los mensajes del un topic, los procesa y grafica en tiempo real.
 * Lenguaje: Python
 * Recursos: VSCode, Kafka Apache, Matplotlib
 * Historial: 
    - Creado el 01.1.2024
    - Modificado el 01.11.2024
"""

import json
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Mapeo inverso para la dirección del viento
wind_mapping_reverse = {0: 'N', 1: 'NO', 2: 'O', 3: 'SO', 4: 'S', 5: 'SE', 6: 'E', 7: 'NE'}

# Función para decodificar los datos de 3 bytes
def decodeData(encoded_data):
    # Convertir de bytes a entero
    encoded_value = int.from_bytes(encoded_data, byteorder='big')
    
    # Extraer cada componente
    temp_encoded = (encoded_value >> 9) & 0x3FFF  # 14 bits para temperatura
    hum_encoded = (encoded_value >> 3) & 0x7F     # 7 bits para humedad
    wind_encoded = encoded_value & 0x07           # 3 bits para dirección del viento
    
    # Decodificar cada valor
    temperature = (temp_encoded / 16383) * 110  # Convertir de vuelta a rango 0-110
    humidity = hum_encoded
    windDirection = wind_mapping_reverse[wind_encoded]
    
    return {
        "temperatura": temperature,
        "humedad": humidity,
        "direccionViento": windDirection
    }

# Inicializar el consumidor de Kafka
consumer = KafkaConsumer(
    '21542',
    group_id='foo2',
    bootstrap_servers=['lab9.alumchat.lol:9092'],
    value_deserializer=lambda x: x  # Recibir los datos como bytes
)

#Listas para almacenar los datos
allTemp = []
allHumi = []

#Configurar el gráfico
plt.ion()
fig, (axTemp, axHumi) = plt.subplots(2, 1, figsize=(6, 6))

#Gráfica de temperatura
axTemp.set_title('Temperatura en tiempo real')
#axTemp.set_xlabel('Muestra')
axTemp.set_ylabel('Temperatura (°C)')
lineTemp, = axTemp.plot([], [], 'r-')

#Gráfica de humedad
axHumi.set_title('Humedad en tiempo real')
axHumi.set_xlabel('Muestra')
axHumi.set_ylabel('Humedad (%)')
lineHumi, = axHumi.plot([], [], 'b-')

#Función para actualizar gráficos en tiempo real
def updateGraphs():
    lineTemp.set_xdata(range(len(allTemp)))
    lineTemp.set_ydata(allTemp)
    
    lineHumi.set_xdata(range(len(allHumi)))
    lineHumi.set_ydata(allHumi)
    
    axTemp.relim()
    axTemp.autoscale_view()
    
    axHumi.relim()
    axHumi.autoscale_view()
    
    plt.pause(0.1)  #Pausa breve para actualizar la ventana gráfica

#Consumo y graficación en tiempo real
try:
    for message in consumer:
        # Decodificar el mensaje
        payload = decodeData(message.value)
        
        allTemp.append(payload['temperatura'])
        allHumi.append(payload['humedad'])

        #Imprimir mensaje recibido
        print(f"Mensaje recibido: {payload}")
        
        #Actualizar gráficos en tiempo real
        updateGraphs()

except KeyboardInterrupt:
    print("Interrumpido por el usuario. Cerrando el consumidor...")
finally:
    plt.ioff()
    #Guardar la gráfica
    plt.savefig("finalGraph.png")
    plt.show()
