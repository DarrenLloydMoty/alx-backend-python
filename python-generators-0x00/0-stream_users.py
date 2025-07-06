import mysql.connector
from seed import connect_to_prodev

def stream_users():
    """
    Generator that fetches rows one by one from the user_data table.
    Yields each user as a dictionary.
    """
    connection = connect_to_prodev()
    if connection:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_data")
        for row in cursor:  # ✅ One loop only!
            yield row
        cursor.close()
        connection.close()
