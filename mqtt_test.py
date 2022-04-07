import time
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("\n\n\nCallback called!\n\n\n")
    if rc == 0:
        # client.connected_flag = True
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}\n")
        
mqtt.Client.connected_flag = False
broker = 'farmer.cloudmqtt.com'
client = mqtt.Client('metering')
client.on_connect = on_connect
client.loop_start()
print(f"Connecting to {broker}")
client.username_pw_set('jhzyqfhe', 'UKnVz6nHdfp2')
client.connect(broker, port=17119)
while not client.is_connected():
# while not client.connected_flag:
    print("In a loop")
    time.sleep(1)

print("Main thread")
client.loop_stop()
client.disconnect()


#cl = connect_mqtt('jhzyqfhe', 'UKnVz6nHdfp2', 'test_meter', 'farmer.cloudmqtt.com', 17119)
#publish(cl, 'TEST_TOPIC')

"""
    Server 	farmer.cloudmqtt.com
    Region 	amazon-web-services::eu-west-1
    Created at 	2019-09-09 19:08 UTC+00:00
    User 	jhzyqfhe
    Password 	UKnVz6nHdfp2
    Port 	17119
    SSL Port 	27119
    Websockets Port (TLS only) 	37119
    Connection limit 	5             
"""