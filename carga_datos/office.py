import socket
import requests
import bcrypt

def obtener_direccion_ip():
    hostname = socket.gethostname()
    direccion_ip = socket.gethostbyname(hostname)
    return direccion_ip

# jobs =  ["scheduler_maintenance", "send_callback", "coa_task_error_recovery", "coa_task_result_sender", "expediente_digital_publisher", "coa_task_sensor", "avance_afi_sender", "task_inbound_by_email", "vericuenta", "verifsim", "task_error_recovery_vericuenta", "email_valida_cuenta", "send_mail_suspendida", "calidad_vericuenta"]
# for job in jobs:
#     r = requests.post(f"http://localhost:8080/api/turn_off/{job}")
#     print(job, r)


def verificar_contrasena(contrasena_ingresada, contrasena_encriptada):
    return bcrypt.checkpw(contrasena_ingresada.encode('utf-8'), contrasena_encriptada.encode('utf-8'))

contrasena_ingresada = '2023emprende'
contrasena_encriptada = '$2b$10$g.zfC8esOEhhpuKuEC0K0u7FnV7HUIjvtMVZiIwnHLJWA1wyCVrAK'

if verificar_contrasena(contrasena_ingresada, contrasena_encriptada):
    print("Contraseña correcta")
else:
    print("Contraseña incorrecta")
    
def encriptar_contrasena(contrasena):
    salt = bcrypt.gensalt()
    contrasena_encriptada = bcrypt.hashpw(contrasena.encode('utf-8'), salt)
    return contrasena_encriptada

print(encriptar_contrasena("2023emprende"))