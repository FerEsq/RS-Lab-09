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

#Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='lab9.alumchat.lol:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Tópico al cual se enviarán los datos 
topic = '21542'

#Envío continuo de datos al tópico de Kafka
try:
    while True:
        #Generar los datos de sensores
        sensorData = generateSensorData()
        
        #Enviar datos al tópico
        producer.send(topic, value=sensorData)
        print(f"Datos enviados: {sensorData}")
        
        #Esperar entre 15 y 30 segundos antes de enviar el próximo mensaje
        time.sleep(random.uniform(15, 30))

except KeyboardInterrupt:
    print("Interrumpido por el usuario. Cerrando el productor...")
finally:
    producer.close()