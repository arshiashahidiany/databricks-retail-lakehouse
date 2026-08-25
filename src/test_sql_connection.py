import pyodbc


connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=DESKTOP-H3LQUJ9;"
    "DATABASE=RetailCo;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)


with pyodbc.connect(connection_string) as connection:

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            @@SERVERNAME AS ServerName,
            DB_NAME() AS DatabaseName,
            COUNT(*) AS CustomerCount
        FROM Customers;
    """)

    row = cursor.fetchone()

    print(f"Server: {row.ServerName}")
    print(f"Database: {row.DatabaseName}")
    print(f"Customers: {row.CustomerCount}")