"""Archivo de configuración del espacio de trabajo para poder configurar oasis"""
from transversal_classes import ProjectParameters
import os
import time


# debe ser quemada porque se asume que los archivos de configuración aun no existen
if not os.path.exists('../parametros_stats/'): 
	os.makedirs('../parametros_stats/')

print("Almacenar los archivos 'config.ini' y 'config.json' en la carpeta 'parametros_stats'" +
	" en el directorio de instalación")

input("Presiona cualquier tecla cuando estos archivos se encuentren en la carpeta 'parametros_stats'...")

print("\n-----------------------------------------------\n")
print("Generando espacio de trabajo, leyendo archivo de configuración" +
	" (config.json), la carpeta 'parametros_stats' debe existir con los archivos"+
	" 'config.ini' y 'config.json' almacenados en ella para que este script pueda" +
	" funcionar, esta carpeta se almacena en el directorio de instalación de OASIS")

time.sleep(10)
# lectura de parametros del archivo json
params = ProjectParameters()
params_other = params.getothergeoparameters()


#generación de carpetas
if not os.path.exists(params.INPUT_FILES): os.makedirs(params.INPUT_FILES)
if not os.path.exists(params.LOG_FILES): os.makedirs(params.LOG_FILES)
if not os.path.exists(params.VIEW_PATH): os.makedirs(params.VIEW_PATH)

if not os.path.exists(params.OUTPUT_GRAPHS_FOLDER): 
	os.makedirs(params.OUTPUT_GRAPHS_FOLDER)
if not os.path.exists(params.PROCESSED_FILES): 
	os.makedirs(params.PROCESSED_FILES)
if not os.path.exists(params.TEMP_INPUT_FILES): 
	os.makedirs(params.TEMP_INPUT_FILES)
if not os.path.exists(params.TEMP_CATALOG_FILES): 
	os.makedirs(params.TEMP_CATALOG_FILES)
if not os.path.exists(params_other['path_to_additional_files']): 
	os.makedirs(params_other['path_to_additional_files'])

# geografias panama
if not os.path.exists(params_other['path_to_storecheck_input_files']): 
	os.makedirs(params_other['path_to_storecheck_input_files'])
if not os.path.exists(params_other['path_to_storecheck_processed_files']): 
	os.makedirs(params_other['path_to_storecheck_processed_files'])
if not os.path.exists(params_other['path_to_storecheck_processed_files']): 
	os.makedirs(params_other['path_to_storecheck_processed_files'])
if not os.path.exists(params_other['path_to_storecheck_processed_files']): 
	os.makedirs(params_other['path_to_storecheck_processed_files'])

print("---------------------------------------------------------------------")
print("\n\n*PASO NECESARIO SOLO PARA LA EJECUCIÓN DE OTRAS GEOGRAFIAS*\n\n" +
	"Creación de carpetas finalizada: Para terminar almacenar el archivo: " +
	f" '{params_other['name_structure_file']}', " +
	f" en la carpeta '{params_other['path_to_additional_files']}.'")
print()
input("Configuración de ambiente finalizada presione cualquier tecla para finalizar...")