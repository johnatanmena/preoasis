# FULL_OASIS_LAB

Proyecto que abarca pruebas de desarrollo desde el trabajo con catálogos hasta el proceso de carga a una base de datos MySQL. El repositorio de creación de la base de datos se encuentra en https://gitlab.com/cabymetal/OASISLAB

### Versión 0.6
Se agrega codigo que permite llevar trazabilidad del submódulo de catálogos, en este módulo se lleva una trazabilidad del proceso de recepción de catálogos. La lista de acciones que se realizan en esta versión son:

* Se agrega el repositorio https://gitlab.com/cabymetal/catalogs como un submódulo del presente proyecto
* Funcionalidad que permite crear catálogos
* Para cada uno de los campos claves del archivo de insumo se genera un catálogo que marca el valor Grupo con el valor Nielsen
* Automáticamente se publican las modificaciones de catálogos al submódulo
* se valida la recepción de los archivos de manera correcta y se les asigna valores por defecto o manuales a los archivos.

### Versión 1.0
Es la primera versión funcional del proyecto, realiza todo el proceso completo de homologación, Extracción y Carga en la base de datos.  Siguiendo las configuraciones del archivo de configuración de JSON. Funcionalidades detalladas:

* Se realiza la estracción del archivo si existen campos claves que no crucen con información de la maestras se lanza un error
* Se realiza el cruce de los campos homologados con los campos de la base de datos para obtener el ID
* Los registros nuevos se agregan en la base de datos y se obtiene el ID.
* Se registran los productos nuevos y a los que se les realizará un update de las características
* y se cargan las transacciones del archivo en la tabla transaccional.

**en estos momentos en el ambiente de ejecución este procceso completo debería tardar 5 minutos máximo**

### Versión 1.1
Se agrega una primera versión del manejo de errores de la aplicación que permite continuar la ejecución en caso de error y almacenar los puntos de error en un archivo aparte para resumir la ejecución solo desde estos puntos.

### Versión 1.2
Se agrega modulo de reporting que se encarga crear logs de ejecución transversales a la herramienta, desde este mismo módulo se puede modificar las variables de creación para que no muestre todo el proceso sino los niveles de **ERROR, CRITICAL, WARNING**, puede ser configurado desde el archivo de parametros.

### Versión 1.8
Se agrega funcionalidad de generar reportes de ejecución y envío de correos a partir de configuración de archivo config.ini. La lista completa de funcionalidades se encuentra listada en:

* Funcionalidad de resumir proceso modificada
* Nueva funcionalidad de Reprocesar carga
* Agregar funcionalidad al estado de ejecución que permite almacenar en archivos csv los estados de ejecución
* Reporte de ejecución en formato HTML que permite ver la ejecución del proceso, en una ventana de tiempo
* Implementar funcionalidad de envio de correos en caso de éxito
* Proceso de lectura y carga de información de un datacheck realizado con pruebas básicas
* Modificaciones en la base de datos para el campo obsservación quede con mayor capacidad de almacenamiento
* Arreglado bug que no escribia archivo de status cuando ocurría un error no clasificado en el proceso principal

#### Versión 1.8.1
Se anexan modificaciones para el funcionamiento de la aplicación que permite la ejecución de manera más sencilla, como un menú de ejecucón que permite seleccionar acción a realizar sin que se cierre la herramienta.
En esta versión se agregan los siguientes cambios:
* Menu de ejecución con las funciones:
* Correcciones generales a nivel de código
* se revisa la lógica de envio  de correo en caso de error que no se generaba de manera correcta para mas de un destinatario
* Se prueba  las funcionalidades de `Reprocesar carga` y `Procesar Novedades`.
* Se verifica la carga de archivo de datacheck a la base de datos (se comprueba el eliminado de datos anteriores e inserción de nuevos)
* Configurar el menu con la función `Borrar valor` que se encontraba deshabilitada para que reciba el nombre del catalogo validado 

Función           | Descripción
----------------- | --------------
Cargar Datos Nielsen A Base de Datos / Cargar Datos Nielsen A FTP     | Permite la carga de datos Nielsen a la base de datos o al FTP (en caso de modo de ejecución Nielsen) <br> realizando todas las fases comentadas en la documentacion
Actualizar catálogos      | Permite modificar un valor de los catálogos generados en el repositorio, dependiendo del modo de ejecución `Nutresa` o `Nielsen` se modificará una de las columnas correspondientes. <br>Se publicará el nuevo valor en el repositorio
Mas Opciones | Despliega opciones adicionales de ejecución
Reprocesar carga | Permite borrar la información transaccional de los archivos que se encuentren en la carpeta `input_files` y recargarla a la base de datos.
Procesar novedades | Permite a partir del último archivo de `status.csv` procesar solo las secciones de los insumos de `input_files` que no pudieron ser procesadas en la carga anterior
Borrar  Valor | Permite borrar el valor de un catálogo, debido a las múltiples consecuencias que esto puede tener en el modelo relacional de momento se encuentra en pruebas.

#### Versión 1.8.2
Se corrigen varios bugs en funcionalidades de configurar catálogos tanto actualizar como borrar valor y se comienzan a almacenar estadísticas. cambios detallados en:
* Mejoras en el menú de ejecución
* Se agregan campos adicionales en los archivos de estadísticas
* Se crea el archivo de estadísticas de catalogos
* En el proceso de actualización se agrega depuración de la base de datos en cambio de valor de catálogo
* En el módulo de carga se realizan modificaciones de proceso de ejecución
* Agregar funcionalidad en la función actualizar catálogo para la maestra de mercados y de niveles

#### Versión 1.8.5
Se revisan varios temas y se agregan funcionalidades de depurar Item Volumen y extraer funcionalidad de extraer información de los diccionarios de Nielsen XML. Se incluye una opción de Generar reporte de ejecución  

### Versión 1.9
Incluidas mejoras visuales y generales al proceso de carga con una barra de carga, mejoras de reportes y generación de arcchivo html de resumen de ejecución con ciertas gráficas que ayudan a la trazabilidad del proceso

## Versión 2.0
Se agrega funcionalidad que permite ver la ejecución como una aplicación web, de esta manera se separa la ejecución en un Front End y en un Backend el Front se encuentra en la carpeta flask_app
`en el archivo main_launcher.py` se encarga de correr los procesos necesarios para la ejecución a modo de servicio con un doble clic

### Versión 2.1
Se agrega funcionalidad que permite categorizar los productos de sctantrack de acuerdo a una condición dada por el negocio. Esto puede generar que los productos aparezcan en la tabla de producto sku como en la tabla de producto agrupado con mismo código de tag pero con diferente código de producto generado por OASIS lo cual es un comportamiento normal dado el nuevo requerimiento, serealizan unas pruebas antes de realizar la carga completa de la base de nuevo.

## Versión 3.0
Cambios mayores al proceso se agrega el proceso de carga de otras geografías y se modifica el archivo de configuración para recibir una mayor cantidad de parámetros, se deben distribuir estos archivos con los procesos correspondientes.

La mayor cantidad de cambios se presentan en el proceso de carga donde se agregan dos nuevas geografías y se modifica el stage area para que reciba nuevas carpetas de insumo cuya finalidad es preparar los datos para llevarlos al área de input_files de OASIS las dos nuevas zonas son: 
Storecheck y Nielsen Panama. Esta última recibe como insumo un Item Volumen y lo procesa para convertirse en un archivo de insumo de OASIS, esta fase aun no se encuentra terminada, sin embargo es funcional para la base de Storecheck.

Esta nueva versión en su procesamiento no modifica ciertos caracteres del campo Rango sin Procesar que son necesarios para algunas fuentes 3 o 4



# Tareas a realizar
- [x] Crear parámetros.
- [x] Generar un reporte de ejecución.
- [x] Capturar el ID de la fuente desde el archivo.
- [x] Crear un módulo de enriquecimiento.
- [x] Leer el nivel ITEM de la base de datos (actualmente esta **quemado en el codigo**)
- [x] Probar funcionamiento incluyendo la publicación y consumo de archivos de insumo del servidor FTP
- [x] Probar la funcionalidad de actualizar catálogos
- [x] Crear un formato para la lectura de actualizaciones 
- [x] Agregar Porcentaje de coincidencia a catalogos temporales
- [x] Cuantos lotes se procesan
- [x] Hora de Inicio y Hora fin de ejecucion archivo
- [x] Cantidad de lotes descartados
- [x] Calcular hora de inicio y de fin proceso de ejecución
- [x] Mover los archivos y de configuracion a la carpeta parametros y stats
- [x] Unificar la variable para cambiar el nivel del Log (DEBUG, ERROR, INFO)
- [x] Ceder control del repositorio
- [x] Implementar envio de correos en caso de error 
- [x] Generar una función que cuente los reprocesos que ha tenido un archivo en una ventana de tiempo dada
- [x] Cuando el modo de ejecución es `procesar novedades` se debe agregar un flag a la función de carga que actualice contra la base de datos
- [x] Actualizar mensajes de ejecución y logs
- [x] validar la ubicación de los archivos de error de catalogos los ERR_OASIS_*
- [x] Verificar el proceso de depuración para mercados, dado que la lógica es muy distinta a la de tablas maestras
- [x] Agregar funcionalidad de Datacheck al menú
- [x] Implementar barra de ejecución
- [x] Separar la función de create_dashboard en subfunciones
- [x] revisar proceso de inserción de información a nivel de Región Canal
- [x] Verificar el funcionamiento del Front en ambiente servidor
- [x] Crear una nueva forma de almacenar los resultados de estadísticas en la base de datos
- [x] En la vista agregar hora de inicio, fase de ejecución, más mensajes de log y debug
- [x] Una vez se carguen históricos verificar los tiempos de carga con el nuevo acercamiento
- [x] crear un log adicional de solo escritura que almacene toda la historia de ejecución.
- [x] Generar un procedimiento para seleccionar el esquema en la clase DB_Utils
- [ ] Modificar la vista de catalogos para obtener un parametro adicional para modificar en otras geografias o en oasis
- [x] Agregar descripción del proceso en la vista storecheck
- [x] Probar que el campo Rango se cargue bien para las fuentes 3 y 4
- [x] Agregar un parametro skipfield al JSON para saber que columnas se deben ignorar en el proceso. De momento solo se ignora RANGO
- [x] Realizar recarga de la fuente de información de la fuente 3 y 4
- [ ] Agregar lógica en el archivo api.py para que verifique el estado de ejecución antes de empezar a ejecutar para poder garantizar que el proceso se ejecuta una sola vez independientemente de la url que lo consuma
- [ ] Revisar y entender el proceso de cambio de tags a partir del proceso de datachecks


# Bugs
- [x] Se debe corregir un bug que  impide que las columnas del archivo se llamen igual que en la base de datos
- [x] Verificar que los catalogos no deben tener valores duplicados
- [x] Implementar una forma segura de conexión a la base de datos
- [x] Implementar en las validaciones el formato de las fechas
- [x] Validar el correcto uso de manejo de memoria para evitar que el proceso se bloquee
- [x] Corregir la lectura de paquetes del código agregando el archivo __init__.py a cada uno
- [x] corregir bug que no borraba los archivos de error de ejecución
- [x] Ejecución de Actualización de catálogo de mercado no revisado.
- [x] Revisar proceso de eliminado o borrado de valor de catálogo
- [x] Error en la función de validaciones cuando existen datos vacios
- [x] Revisar mensajes de ejecución del tablero de control
- [x] Revisar la lógica de selección de fechas en la creación del tablero y en la función de creación de archivo HTML de reporte
- [x] Revisar casos de inserción parcial
- [x] Corregido bug que no almacenaba tamano sin procesar cuando existía un valor numérico
- [x] Revisar estadísticas se duplican con cada ejecución
- [x] Revisar campo fecha de la fuente 3 y verificar que el rango sea el adecuado