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

def return_ff():
    response = b'\x82\xc6.\xc6(00000000\xa9\x03'
    return response

def return_table1():
    response = b'\x02F.F(00000000)\r\n0.0.0(10067967)\r\n0.0.1(10067967)\r\n0.9.1(202405)\r\n0.9.2(221113)\r\n0.1.0(12)\r\n0.1.2(2211010000)\r\n0.1.2*12(2211010000)\r\n0.1.2*11(2210010000)\r\n0.1.2*10(2209010000)\r\n1.6.1(0.50262*kW)(2211120730)\r\n1.6.1*12(0.39912*kW)(2210130900)\r\n1.6.1*11(0.74906*kW)(2209281400)\r\n1.6.1*10(0.49578*kW)(2208111330)\r\n2.6.1(0.00000*kW)(2211010000)\r\n2.6.1*12(0.00000*kW)(2210010000)\r\n2.6.1*11(0.00000*kW)(2209010000)\r\n2.6.1*10(0.00000*kW)(2208010000)\r\n1.8.0(01281.6601*kWh)\r\n1.8.0*12(01236.1958*kWh)\r\n1.8.0*11(01158.9747*kWh)\r\n1.8.0*10(01097.4085*kWh)\r\n2.8.0(00000.0000*kWh)\r\n2.8.0*12(00000.0000*kWh)\r\n2.8.0*11(00000.0000*kWh)\r\n2.8.0*10(00000.0000*kWh)\r\n5.8.0(00049.1785*kvarh)\r\n5.8.0*12(00048.8006*kvarh)\r\n5.8.0*11(00045.9754*kvarh)\r\n5.8.0*10(00041.7958*kvarh)\r\n6.8.0(00000.0000*kvarh)\r\n6.8.0*12(00000.0000*kvarh)\r\n6.8.0*11(00000.0000*kvarh)\r\n6.8.0*10(00000.0000*kvarh)\r\n7.8.0(00000.0000*kvarh)\r\n7.8.0*12(00000.0000*kvarh)\r\n7.8.0*11(00000.0000*kvarh)\r\n7.8.0*10(00000.0000*kvarh)\r\n8.8.0(00079.9454*kvarh)\r\n8.8.0*12(00075.0837*kvarh)\r\n8.8.0*11(00062.7016*kvarh)\r\n8.8.0*10(00050.2358*kvarh)\r\n0.3.3(3000)\r\n0.2.2(00000001)\r\n0.2.0(01.01.28)\r\n0.2.0(02.02.13)\r\n0.2.0(2.2.8)\r\n!\r\n\x03'
    return response

def return_p98():
    # TODO : finish me
    response = b''
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
            elif b'F.F' in data:
                response = return_ff()
            else:
                response(f'{data} received but not defined')
            print(f'<<< {response}')
            connection.sendall(response)
        else:
            print(f'No data, break')
            break                    


