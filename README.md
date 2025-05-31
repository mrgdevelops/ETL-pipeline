# ETL-pipeline

GCP Project - ETL pipeline - Handling Transportation Data

    Proyecto:

Pipeline ETL (Extract, Transform, Load) en Google Cloud.

    Propósito:

Realizar proceso ETL completo: extracción, transformación y carga de datos en Google Cloud Platform (GCP) utilizando dataset "transportation_data.json" de datos sintéticos.
Asimismo con la presente descripción se mustra que el proyecto queda bien documentado, con explicaciones paso por paso de su lógica interna y servicios / tecnologías utilizadas.

    Pre-requisitos:

- simplicidad y eficacia (minimizar time-to-market)
- escalabilidad
- serverless
- event-driven
- cost-efficient

      Descripción del proyecto:

  Este proyecto se centra en utilizar servicios gestionados (managed) y sin servidor (serverless) de GCP para minimizar la complejidad operativa, el mantenimiento y los costos, lo cual lo hace sencillo y eficaz.

---

    	Tecnologías y servicios de GCP que se utilizan en este proyecto:

    • Google Cloud Storage (GCS) - Data Lake:

Propósito: Sirve como la capa de almacenamiento para nuestros datos brutos (transportation_data.json) y también como el destino para los datos transformados (en formato CSV). Es un almacenamiento de objetos escalable y de bajo costo.
Fase: Extract (origen) y Load (destino final para datos limpios).

    • Cloud Functions:

Propósito: Actua como el motor de la fase de Transformación (T). Una función serverless que se activará automáticamente cuando se suba un nuevo archivo JSON a Cloud Storage. Ejecutará el código de Python (o Node.js, etc.) para realizar las transformaciones.
Fase: Transform (T).

    • BigQuery (Warehouse):

Propósito: El destino final de datos limpios y transformados, listos para análisis y visualización. Es un data warehouse serverless, escalable y muy rápido para consultas SQL.
Fase: Load (L).

    • Python, pandas (lógica de Cloud Functions y traqnsformación de datos)

    • GenAI (Gemini: generación de datos sintéticos, promt engineering)

    • IAM (permisos)

    • Google Cloud API's (activar las necesarias para la interacción de servicios)

---

    	Flujo del Pipeline ETL (Sencillo y Eficaz)

A continuación se detalla el flujo paso a paso, abarcando las fases Extract, Transform y Load:

    1. Extract (E):

Paso 1.1: Subida del archivo de origen.
El archivo transportation_data.json se subirá a un bucket específico de Cloud Storage que se designa como bucket de datos brutos o staging.
Ejemplo: gs://grupo-ruiz-raw-data/transportation_data.json

    2. Transform (T):

Paso 2.1: Activación de Cloud Function.
Cuando transportation_data.json es subido al bucket gs://grupo-ruiz-raw-data/, se activará automáticamente una Cloud Function.
Esta Cloud Function leerá el contenido del archivo JSON desde el bucket.
Paso 2.2: Lógica de Transformación en la Cloud Function.
Dentro de la Cloud Function, se ejecutará el código Python (utilizando la biblioteca Pandas) que realizará las siguientes transformaciones:
Lectura del JSON: Cargar el JSON en un formato manejable (un DataFrame de Pandas en Python).
Manejo de Nulos:
pais_operacion: Rellenar nulos con un valor por defecto (ej: "Desconocido") o eliminar registros.
numero_viajeros: Rellenar nulos con 0 o la media, o eliminar registros. Convertir "Cuarenta" a 40.
modelo_autocar, descripcion_averia, costo_averia_eur: Rellenar nulos con "N/A" o 0 respectivamente si no hay avería.
Limpieza y Conversión de Tipos:
distancia_km y tarifa_media_por_viajero_eur: Reemplazar comas por puntos y convertir a float.
puntuacion_cliente: Limitar valores a 1-5 (ej: 6 se convierte a 5) y asegurar que sea un entero.
incidencia_averia: Convertir a booleano o entero (0/1).
Estandarización de Texto:
pais_operacion: Unificar a "España", "Portugal", "Marruecos".
marca_autocar, modelo_autocar: Convertir a title case o lowercase para consistencia.
Campos Derivados (Opcional pero recomendado para ETL completo):
retraso_minutos: Calcular la diferencia en minutos entre hora_llegada_real y hora_salida_programada. (Esto requeriría convertir las horas a formato de tiempo y calcular la diferencia).
velocidad_media_kmh: distancia_km / (tiempo_viaje_minutos / 60).
Los datos transformados se convertirán a un formato CSV que BigQuery pueda ingerir eficientemente.
Paso 2.3: Almacenamiento temporal de datos transformados.
La Cloud Function guardará el archivo de datos transformados en otro bucket de Cloud Storage, que designamos como nuestro bucket de datos limpios o procesados: gs://grupo-ruiz-processed-data/transportation_data_transformed_YYYYMMDDHHMMSS.csv

    3. Load (L):

Paso 3.1: Carga en BigQuery.
Una vez que el archivo CSV transformado se guarde en gs://grupo-ruiz-processed-data/, se configura una tarea de carga de BigQuery (BigQuery load job), que es ejecutada después de guardar el archivo (arquitectura event-driven).
BigQuery inferirá AUTOMÁTICAMENTE el esquema de los datos CSV.
Los datos se cargarán en una tabla designada en BigQuery: grupo_ruiz_dataset.viajes_autobuses_limpios.

---

    	Diagrama de Flujo:

"transportation_data.json"
|
v
[Cloud Storage: gs://grupo-ruiz-raw-data/] (EVENT TRIGGER)
|
v
[Cloud Function: 'process_transport_data']
| - Lee .JSON
| - Realiza Transformaciones (limpieza de nulos, tipos, estandarización, cálculos)
| - Genera .CSV
v
[Cloud Storage: gs://grupo-ruiz-processed-data/]
|
v
[BigQuery Load Job (ejecutado por un trigger, "event-driven")]
|
v
[BigQuery Table: `grupo_ruiz_dataset.viajes_autobuses_limpios`]
|
v
[Looker Studio (para visualización)]

---

    VENTAJAS de esta arquitectura:

Serverless: No es necesario gestionar servidores. GCP se encarga de la infraestructura.
Escalable: Cloud Storage y BigQuery escalan automáticamente. Cloud Functions escala según la demanda.
Costo-efectivo: Pagas solo por el almacenamiento, la ejecución de la función y los datos procesados/almacenados en BigQuery. Para un archivo de 200 filas, los costos serán mínimos, casi despreciables. Es más, este proyecto en concreto utiliza Free Tier de GCP, con lo que es EXTREMELY cost-effective ;)
Sencillez: La lógica ETL se concentra en un único script dentro de la Cloud Function.
