import socket
import time

tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_address = ('localhost', 8000)
tcp_socket.bind(server_address)

receive_timeout = 2
tcp_socket.listen()


def id_message():

    return b'/MCS5\\@0050010067967\r\n\x03'

def return_b0():

    return b'B0\x03'

def return_p01():
    response = b'\x02P.01(1220401010000)(08)(15)(6)(1-0:1.5.0)(kW)(1-0:2.5.0)(kW)(1-0:5.5.0)(kvar)(1-0:6.5.0)(kvar)(1-0:7.5.0)(kvar)(1-0:8.5.0)(kvar)\r\n(0.30499)(0.00000)(0.00000)(0.00000)(0.00000)(0.00728)\r\n(0.31774)(0.00000)(0.00001)(0.00000)(0.00000)(0.00480)\r\n(0.33878)(0.00000)(0.00206)(0.00000)(0.00000)(0.00366)\r\n(0.30506)(0.00000)(0.00000)(0.00000)(0.00000)(0.00713)\r\n\x03'
    return response

def return_on_ack():

    return b'\x01P0\x02(10067967)\x03'

while True:
    connection, client = tcp_socket.accept()

    while True:
        time.sleep(0.5)
        # Need to process the sequence
        data = connection.recv(32)
        if data:
            print(f'>>> {data}')
            if data.startswith(b'/?') and data.endswith(b'!\r\n'):
                # ID message
                response = id_message()
                #response = return_b0()
            elif b'P.01(' in data:
                # P.01
                response = return_p01()
            elif b'\x06051\r\n' in data:
                response = return_on_ack()
            else:
                response(f'{data} received but not defined')
            print(f'<<< {response}')
            connection.sendall(response)
        else:
            print(f'No data, break')
            break                    


