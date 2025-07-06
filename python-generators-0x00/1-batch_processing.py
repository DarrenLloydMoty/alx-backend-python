import mysql.connector
from seed import connect_to_prodev

def stream_users_in_batches(batch_size):
    """
    Generator that yields user data in batches from the user_data table.
    """
    connection = connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    
    offset = 0
    while True:
        cursor.execute(
            "SELECT * FROM user_data LIMIT %s OFFSET %s", (batch_size, offset)
        )
        rows = cursor.fetchall()
        if not rows:
            break
        yield rows
        offset += batch_size

    cursor.close()
    connection.close()

def batch_processing(batch_size):
    """
    Process each batch by filtering users older than 25 and printing them.
    """
    for batch in stream_users_in_batches(batch_size):  # Loop 1
        for user in batch:  # Loop 2
            if user['age'] > 25:
                print(user)
