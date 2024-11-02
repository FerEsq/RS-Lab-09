"""
 * Nombre: sensorSimulation.py
 * Autores:
    - Fernanda Esquivel, 21542
    - Melissa Pérez, 21385
 * Descripción: Simula la generación de datos de sensores de temperatura, humedad y dirección del viento.
 * Lenguaje: Python
 * Recursos: VSCode, Kafka Apache
 * Historial: 
    - Creado el 19.08.2024
    - Modificado el 02.10.2024
"""

import random
import json

def generateSensorData():
    #Generar temperatura con distribución normal centrada en 55, límite [0, 110]
    temperature = round(max(0, min(110, random.normalvariate(55, 15))), 2)
    
    #Generar humedad con distribución normal centrada en 55, límite [0, 100]
    humidity = int(max(0, min(100, random.normalvariate(55, 15))))
    
    #Generar dirección del viento de manera uniforme
    windDirections = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    windDirection = random.choice(windDirections)
    
    #Crear el diccionario de datos
    data = {
        "temperatura": temperature,
        "humedad": humidity,
        "direccionViento": windDirection
    }
    
    #Convertir a JSON
    jsonData = json.dumps(data, ensure_ascii=False)
    return jsonData

#Ejemplo de uso
for _ in range(5):  #Genera cinco muestras como ejemplo
    print(generateSensorData())
