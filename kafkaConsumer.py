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

#Inicializar el consumidor de Kafka
consumer = KafkaConsumer(
    '21542',
    group_id='foo2',
    bootstrap_servers=['lab9.alumchat.lol:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
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
        #Procesar el mensaje y extraer datos
        payload = message.value
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
