import json
import os
import pandas as pd
from datetime import datetime, time, timedelta
from google.cloud import storage, bigquery

# Initialize Google Cloud Storage and BigQuery clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Configuration (Environment variables to set in Cloud Function deployment)
RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME')
PROCESSED_BUCKET_NAME = os.environ.get('PROCESSED_BUCKET_NAME')
BIGQUERY_PROJECT_ID = os.environ.get('BIGQUERY_PROJECT_ID')
BIGQUERY_DATASET_ID = 'grupo_ruiz_dataset'
BIGQUERY_TABLE_ID = 'viajes_autobuses_limpios'

# Define the exact order of columns for the DataFrame to match the BigQuery schema
# This list must match the BigQuery schema definition below
BQ_SCHEMA_COLUMNS = [
    "id_viaje",
    "fecha_viaje",
    "hora_salida_programada",
    "hora_llegada_real",
    "origen_ciudad",
    "destino_ciudad",
    "pais_operacion",
    "numero_viajeros",
    "distancia_km",
    "tiempo_viaje_minutos",
    "tarifa_media_por_viajero_eur",
    "marca_autocar",
    "modelo_autocar",
    "matricula_autocar",
    "tipo_servicio",
    "incidencia_averia",
    "descripcion_averia",
    "costo_averia_eur",
    "puntuacion_cliente",
    "combustible_consumido_litros",
    "id_conductor",
    "edad_conductor",
    "retraso_minutos",          # Derived field
    "velocidad_media_kmh"       # Derived field
]


def etl_transportation_data(request):
    """
    Triggered by a Cloud Storage bucket event via Cloud Run Functions.
    Performs ETL operations on transportation data.

    Args:
        request (flask.Request): The request object containing the event payload.
                                 For GCS events, the event data is in request.json.
    """
    # Parse the Cloud Storage event data from the request body
    if not request.is_json:
        print("Received a request that is not JSON. Skipping.")
        return 'Not JSON', 400 # Return error for non-JSON requests

    event_data = request.get_json()

    # Extract event details - these keys are standard for GCS events
    file_name = event_data.get('name')
    bucket_name = event_data.get('bucket')

    if not file_name or not bucket_name:
        print(f"Error: Missing 'name' or 'bucket' in event data: {event_data}")
        return 'Missing file/bucket info', 400

    if not file_name.endswith('.json'):
        print(f"Skipping file {file_name}: Not a JSON file.")
        return 'Not a JSON file', 200 # Return OK as it's not an error, just skipped

    # Optional: Verify if the bucket is the expected raw bucket
    if bucket_name != RAW_BUCKET_NAME:
        print(f"Skipping file {file_name} from unexpected bucket: {bucket_name}. Expected: {RAW_BUCKET_NAME}")
        return 'Unexpected bucket', 200

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # E: Extract
    # ----------------------------------------------------------------------
    source_blob = storage_client.bucket(bucket_name).blob(file_name)
    try:
        data_json = source_blob.download_as_string()
        data = json.loads(data_json)
        df = pd.DataFrame(data)
        print("Extraction complete. Data loaded into DataFrame.")
    except Exception as e:
        print(f"Error during extraction or JSON parsing for {file_name}: {e}")
        raise

    # T: Transform
    # ----------------------------------------------------------------------
    print("Starting data transformation...")

    # 1. Handle Nulos / Missing Values
    # Fill NaN for string columns with 'Desconocido' or 'N/A'
    df['pais_operacion'] = df['pais_operacion'].fillna('Desconocido')
    df['modelo_autocar'] = df['modelo_autocar'].fillna('N/A')
    df['descripcion_averia'] = df['descripcion_averia'].fillna('Sin Avería')

    # Convert 'numero_viajeros', replace 'Cuarenta' with 40, fill NaN with 0, then to int
    df['numero_viajeros'] = df['numero_viajeros'].replace('Cuarenta', 40).fillna(0).astype(int)

    # Fill NaN for numeric cost with 0.0
    df['costo_averia_eur'] = df['costo_averia_eur'].fillna(0.0)

    # 2. Clean and Convert Data Types
    # Convert 'distancia_km' and 'tarifa_media_por_viajero_eur' to float, handling comma decimal
    df['distancia_km'] = df['distancia_km'].astype(str).str.replace(',', '.', regex=False).astype(float)
    df['tarifa_media_por_viajero_eur'] = df['tarifa_media_por_viajero_eur'].astype(str).str.replace(',', '.', regex=False).astype(float)

    # Ensure 'incidencia_averia' is boolean (True/False)
    df['incidencia_averia'] = df['incidencia_averia'].astype(bool)

    # Clean 'puntuacion_cliente': ensure it's int and within 1-5 range
    df['puntuacion_cliente'] = pd.to_numeric(df['puntuacion_cliente'], errors='coerce').fillna(0).astype(int)
    df['puntuacion_cliente'] = df['puntuacion_cliente'].clip(1, 5) # Clip values to range [1, 5]

    # Convert 'fecha_viaje' to date objects for consistency
    df['fecha_viaje'] = pd.to_datetime(df['fecha_viaje']).dt.date

    # Ensure other numeric types
    df['tiempo_viaje_minutos'] = df['tiempo_viaje_minutos'].astype(int)
    df['combustible_consumido_litros'] = df['combustible_consumido_litros'].astype(float)
    df['edad_conductor'] = df['edad_conductor'].astype(int)

    # 3. Standardize Text
    country_mapping = {
        'España': 'España', 'ESPAÑA': 'España', 'ES': 'España',
        'Portugal': 'Portugal', 'PT': 'Portugal',
        'Marruecos': 'Marruecos', 'MARRUECOS': 'Marruecos', 'MAR': 'Marruecos',
        'Desconocido': 'Desconocido'
    }
    df['pais_operacion'] = df['pais_operacion'].apply(lambda x: country_mapping.get(x, x)).astype(str)

    df['marca_autocar'] = df['marca_autocar'].astype(str).str.title()
    df['modelo_autocar'] = df['modelo_autocar'].astype(str).str.title()
    df['origen_ciudad'] = df['origen_ciudad'].astype(str).str.title()
    df['destino_ciudad'] = df['destino_ciudad'].astype(str).str.title()
    df['tipo_servicio'] = df['tipo_servicio'].astype(str).str.title()

    # 4. Derived Fields (ETL - Business Logic)

    # Calculate Retraso_minutos (Delay in minutes)
    def calculate_delay(scheduled_str, actual_str, travel_time_min):
        try:
            if not isinstance(scheduled_str, str) or not scheduled_str or \
               not isinstance(actual_str, str) or not actual_str:
                return None

            scheduled_time = datetime.strptime(scheduled_str, '%H:%M').time()
            actual_time = datetime.strptime(actual_str, '%H:%M').time()

            dummy_date = datetime(2000, 1, 1) # Use a dummy date for time calculations
            scheduled_dt = datetime.combine(dummy_date, scheduled_time)
            actual_dt = datetime.combine(dummy_date, actual_time)

            # If actual arrival is on the next day (e.g., scheduled 23:00, actual 01:00)
            if actual_dt < scheduled_dt:
                actual_dt += timedelta(days=1)

            calculated_travel_time = (actual_dt - scheduled_dt).total_seconds() / 60
            delay = calculated_travel_time - travel_time_min
            return int(delay) if delay > 0 else 0 # Only positive delays, or 0 if on time/early
        except (ValueError, TypeError) as e:
            print(f"Warning: Could not calculate delay for times {scheduled_str}-{actual_str} (travel time {travel_time_min}): {e}")
            return None # Return None if calculation fails

    # Apply the function row-wise
    df['retraso_minutos'] = df.apply(
        lambda row: calculate_delay(row['hora_salida_programada'], row['hora_llegada_real'], row['tiempo_viaje_minutos']),
        axis=1
    )


    # Calculate Velocidad_media_kmh (Average speed)
    df['velocidad_media_kmh'] = df.apply(
        lambda row: (row['distancia_km'] / (row['tiempo_viaje_minutos'] / 60)) if row['tiempo_viaje_minutos'] > 0 else 0,
        axis=1
    )
    # Convert infinite values (from division by zero) or NaN to None for BigQuery NULL compatibility
    df['velocidad_media_kmh'] = df['velocidad_media_kmh'].replace([float('inf'), -float('inf')], pd.NA)
    df['velocidad_media_kmh'] = df['velocidad_media_kmh'].fillna(pd.NA) # Replace NaN with pandas NA

    print("Transformation complete.")

    # --- Crucial step: Reorder DataFrame columns to match BigQuery schema ---
    # This ensures the CSV columns are in the exact order BigQuery expects.
    try:
        df = df[BQ_SCHEMA_COLUMNS]
        print("DataFrame columns reordered to match BigQuery schema.")
    except KeyError as e:
        print(f"Error: Column mismatch during reordering. Missing column: {e}. Check BQ_SCHEMA_COLUMNS vs DataFrame columns.")
        # Print actual DataFrame columns to help diagnose
        print("Actual DataFrame columns:", df.columns.tolist())
        raise # Re-raise to fail the function if columns don't match

    # --- Debugging: Print DataFrame info before CSV export ---
    print("\n--- DataFrame Info before CSV Export ---")
    print("DataFrame columns order (final):", df.columns.tolist())
    print("DataFrame dtypes (final):")
    print(df.dtypes)
    print("First 5 rows of DataFrame (final):")
    print(df.head().to_string()) # .to_string() prevents truncation
    print("--- End DataFrame Info ---")


    # L: Load (to GCS as CSV first, then to BigQuery)
    # ----------------------------------------------------------------------

    # Define output file path in the processed bucket
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_name = f"transportation_data_transformed_{timestamp}.csv"
    destination_blob_path = f"{output_file_name}"

    # Save DataFrame to CSV in memory
    # `na_rep=''` ensures NaN values are represented as empty strings for NULLABLE columns in CSV
    csv_data = df.to_csv(index=False, na_rep='')
    processed_bucket = storage_client.bucket(PROCESSED_BUCKET_NAME)
    processed_blob = processed_bucket.blob(destination_blob_path)

    try:
        processed_blob.upload_from_string(csv_data, content_type='text/csv')
        print(f"Transformed data saved to gs://{PROCESSED_BUCKET_NAME}/{destination_blob_path}")
    except Exception as e:
        print(f"Error saving transformed CSV to GCS: {e}")
        raise

    # Load CSV from processed bucket into BigQuery
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1, # Skip CSV header row
        autodetect=False, # Use predefined schema
        schema=bigquery_client.get_table(table_id).schema, # Get the schema from the existing table
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Append to table
        # max_bad_records=100, # Uncomment this line temporarily for debugging if errors persist
    )

    uri = f"gs://{PROCESSED_BUCKET_NAME}/{destination_blob_path}"

    try:
        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        print(f"Starting BigQuery load job {load_job.job_id}...")
        load_job.result() # Wait for the job to complete
        print(f"BigQuery load job finished. {load_job.output_rows} rows loaded into {table_id}.")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")
        raise

    return 'OK', 200 # Return a success response for HTTP trigger