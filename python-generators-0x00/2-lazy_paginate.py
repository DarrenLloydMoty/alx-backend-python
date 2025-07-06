from seed import connect_to_prodev

def paginate_users(page_size, offset):
    """
    Fetches a page of users from the database with the given page size and offset.
    """
    connection = connect_to_prodev()
    cursor = connection.cursor(dictionary=True)

    cursor.execute(
        f"SELECT * FROM user_data LIMIT {page_size} OFFSET {offset}"
    )
    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    return rows

def lazy_paginate(page_size):
    """
    Generator function that lazily yields each page of users.
    Only one loop is used, and paginate_users is called inside.
    """
    offset = 0
    while True:
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size
