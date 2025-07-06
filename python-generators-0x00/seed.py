import mysql.connector
import csv

def connect_db():
    """Connect to MySQL server (no database)."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="your_mysql_user",
            password="your_mysql_password",
            autocommit=True
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def create_database(connection):
    """Create database ALX_prodev if not exists."""
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev;")
    cursor.close()

def connect_to_prodev():
    """Connect to ALX_prodev database."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="your_mysql_user",
            password="your_mysql_password",
            database="ALX_prodev",
            autocommit=True
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def create_table(connection):
    """Create user_data table if not exists."""
    cursor = connection.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        user_id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age DECIMAL NOT NULL
    );
    """
    cursor.execute(create_table_query)
    cursor.close()
    print("Table user_data created successfully")

def insert_data(connection, csv_file):
    """Insert data from CSV file into user_data table."""
    cursor = connection.cursor()

    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check if user_id already exists to avoid duplicates
            cursor.execute("SELECT user_id FROM user_data WHERE user_id = %s", (row['user_id'],))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)",
                    (row['user_id'], row['name'], row['email'], float(row['age']))
                )
        connection.commit()
    cursor.close()
    print("Data inserted successfully")
