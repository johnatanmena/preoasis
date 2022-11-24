#launcher de ejecución que permite ejecutar oasis en modo front y backend muestra el link donde se está ejecutando la aplicación entre otras
import os
import subprocess
import time
import webbrowser as wb

start_time = time.time()

DETACHED_PROCESS = 0x00000008
if os.name == 'nt':
  proc1 = subprocess.Popen("python api.py &", shell=True, stdin=None, stdout=None, stderr=None, close_fds=True,creationflags=DETACHED_PROCESS).pid
  proc2 = subprocess.Popen("python ../flask_app/app.py &", shell=True, stdin=None, stdout=None, stderr=None, close_fds=True,creationflags=DETACHED_PROCESS).pid #on windows & is mandatory
  print("Lanzando OASIS front y backend, preparando ambiente de ejecución")
  print(proc1)
  print(proc2)
  time.sleep(10) #dar un poco de tiempo a inicializar los servicios

  #verificar que los procesos se encuentren activos
  try:
    os.kill(proc1, 0)
  except OSError:
    print("El proceso de backend no se encuentra activo, verificar mensajes de log")
  else:
    print("Backend en funcionamiento")


  try:
    os.kill(proc2, 0)
  except OSError as e:
    print("El proceso de front no se encuentra activo, verificar ejecución manual")
    print(e)
  else:
    print("Proceso activo en http://127.0.0.1:5000/")
    print("Listo para ejecución en el explorador de internet predeterminado")
    wb.open("http://127.0.0.1:5000/") #se abre la aplicación desde el servidor
else:
  proc1 = subprocess.Popen(["python api.py &"], shell=True).pid
  proc2 = subprocess.call(["python ../flask_app/app.py"], shell=True) #correr proceso
  print("Lanzando OASIS front y backend, preparando ambiente de ejecución, puerto 5000")
  print(proc1)
  time.sleep(10) #dar un poco de tiempo a inicializar los servicios
  
  #verificar que los procesos se encuentren activos
  try:
    os.kill(proc1, 0)
  except OSError:
    print("El proceso de backend no se encuentra activo, verificar mensajes de log")
  else:
    print("Backend en funcionamiento")


print("procesos lanzados")

## codigo para verificar que 
time.sleep(30)