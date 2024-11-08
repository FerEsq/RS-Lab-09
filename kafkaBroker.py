"""
 * Nombre: kafkaBroker.py
 * Autores:
    - Fernanda Esquivel, 21542
    - Melissa Pérez, 21385
 * Descripción: Simula la generación de datos de sensores de temperatura, humedad y dirección del viento y los envía a un Topic de Kafka.
 * Lenguaje: Python
 * Recursos: VSCode, Kafka Apache
 * Historial: 
    - Creado el 01.1.2024
    - Modificado el 01.11.2024
"""

import json
import random
import time
from kafka import KafkaProducer

#Función para generar datos de los sensores
def generateSensorData():
    #Generar temperatura y humedad con distribución normal
    temperature = round(max(0, min(110, random.normalvariate(55, 15))), 2)
    humidity = int(max(0, min(100, random.normalvariate(55, 15))))
    
    #Generar dirección del viento de manera aleatoria
    windDirections = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    windDirection = random.choice(windDirections)
    
    #Crear los datos en formato JSON
    data = {
        "temperatura": temperature,
        "humedad": humidity,
        "direccionViento": windDirection
    }
    
    return data

# Función para codificar los datos en 3 bytes
def encodeData(data):
    # Codificar la temperatura en 14 bits (0 a 110 equivale a un rango de 0 a 16383)
    temp_encoded = int((data['temperatura'] / 110) * 16383)
    
    # Codificar la humedad en 7 bits (0 a 100%)
    hum_encoded = data['humedad']
    
    # Codificar la dirección del viento en 3 bits (8 direcciones)
    wind_mapping = {'N': 0, 'NO': 1, 'O': 2, 'SO': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}
    wind_encoded = wind_mapping[data['direccionViento']]
    
    # Combinar todos los valores en 3 bytes
    encoded_value = (temp_encoded << 9) | (hum_encoded << 3) | wind_encoded
    return encoded_value.to_bytes(3, byteorder='big')

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='lab9.alumchat.lol:9092',
    value_serializer=lambda v: v # Enviar los datos como bytes
)

#Tópico al cual se enviarán los datos 
topic = '21542'

#Envío continuo de datos al tópico de Kafka
try:
    while True:
        #Generar los datos de sensores
        sensorData = generateSensorData()
        
        # Codificar los datos antes de enviarlos
        encoded_data = encodeData(sensorData)
        
        # Enviar datos codificados al tópico
        producer.send(topic, value=encoded_data)
        print(f"Datos enviados: {sensorData} como {encoded_data}")
        
        #Esperar entre 15 y 30 segundos antes de enviar el próximo mensaje
        time.sleep(random.uniform(15, 30))

except KeyboardInterrupt:
    print("Interrumpido por el usuario. Cerrando el productor...")
finally:
    producer.close()