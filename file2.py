from flask import Flask, request, jsonify
import pandas as pd
import mysql.connector
import requests
import sys
import atexit

# Flask app initialization
app = Flask(__name__)

# MySQL Configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'root'  # Replace with your MySQL password
DB_NAME = 'iot_data'

# ThingSpeak Configuration
THING_SPEAK_API_KEY = "H0Z0SLRC9WRILQAA"  # Replace with your ThingSpeak API key
THING_SPEAK_URL = "https://api.thingspeak.com/update"

# Limit maximum upload size (e.g., 10 MB)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10 MB

# Suppress shutdown errors during interpreter exit
def suppress_shutdown_error(exc_type, exc_value, exc_traceback):
    if exc_type == SystemError:
        print("SystemError during shutdown ignored.")
    else:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = suppress_shutdown_error

# Cleanup resources on app exit
def cleanup():
    print("Application shutting down. Cleaning up resources...")

atexit.register(cleanup)

@app.route('/')
def home():
    return "Flask server for IoT is running!"

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/upload-dataset', methods=['POST'])
def upload_dataset():
    connection = None
    try:
        # Log request initiation
        print("Received a request at '/upload-dataset'")

        # Check if a file is provided
        if 'file' not in request.files:
            print("No file provided in the request.")
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            print("No file selected.")
            return jsonify({"error": "No file selected"}), 400

        # Load the dataset into pandas DataFrame
        print("Processing uploaded dataset...")
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Failed to read the file: {e}")
            return jsonify({"error": f"Invalid file format: {str(e)}"}), 400

        print("Dataset loaded successfully!")

        # Clean column names to make them SQL-compatible
        df.columns = [col.strip().replace(" ", "_").replace("+", "_").replace(":", "_") for col in df.columns]

        # Connect to MySQL database
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()

        # Ensure the table exists
        table_name = 'dynamic_sensor_data'
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY);")

        # Add columns dynamically
        type_mapping = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)'
        }
        for column, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE '{column}';")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{column}` {sql_type};")

        # Insert data into the table
        print("Inserting data into MySQL...")
        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            columns = ", ".join([f"`{col}`" for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", tuple(row))

        connection.commit()

        # Send the first row to ThingSpeak
        if not df.empty:
            print("Sending first row to ThingSpeak...")
            payload = {f"field{i+1}": value for i, value in enumerate(df.iloc[0].values)}
            payload["api_key"] = THING_SPEAK_API_KEY
            response = requests.post(THING_SPEAK_URL, params=payload)
            if response.status_code != 200:
                print("ThingSpeak upload failed:", response.text)
                return jsonify({"message": "Data uploaded to MySQL, but ThingSpeak upload failed"}), 200

        print("Dataset uploaded successfully to MySQL and ThingSpeak!")
        return jsonify({"message": "Dataset uploaded successfully to MySQL and ThingSpeak"}), 200

    except mysql.connector.Error as db_error:
        print(f"MySQL error occurred: {db_error}")
        return jsonify({"error": f"MySQL error: {str(db_error)}"}), 500

    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

    finally:
        # Ensure all resources are properly closed
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
        print("Resources cleaned up successfully.")

if __name__ == "__main__":
    print("Starting Flask server...")
    print("Routes registered:", app.url_map)
    app.run(debug=True, port=5001)
