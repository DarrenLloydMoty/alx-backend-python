from seed import connect_to_prodev

def stream_user_ages():
    """
    Generator that yields user ages one by one from the database.
    """
    connection = connect_to_prodev()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("SELECT age FROM user_data")
    for row in cursor:
        yield row['age']

    cursor.close()
    connection.close()

def calculate_average_age():
    """
    Uses the stream_user_ages generator to compute average age
    without loading all data into memory.
    """
    total_age = 0
    count = 0

    for age in stream_user_ages():  # Loop 1: iterate generator
        total_age += age
        count += 1

    average_age = total_age / count if count else 0
    print(f"Average age of users: {average_age:.2f}")

if __name__ == "__main__":
    calculate_average_age()
