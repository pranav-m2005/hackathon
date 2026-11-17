import mysql.connector as mc

connection = None
cursor = None

try:
    connection = mc.connect(
        host="127.0.0.1",         # Your Mac's Wi-Fi IP
        port=3306,
        user="demo",
        password="demopassword",
        database="demo_one",
         connection_timeout=10
    )

    if connection.is_connected():
        print("MySQL connected successfully!")

        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE(), CURRENT_USER(), NOW()")

        result = cursor.fetchone()
        print("Database:", result[0])
        print("User:", result[1])
        print("Time:", result[2])

except mc.Error as error:
    print("Connection failed:", error)

finally:
    if cursor is not None:
        cursor.close()

    if connection is not None and connection.is_connected():
        connection.close()
        print("Connection closed.")

