import mysql.connector
from mysql.connector import Error
import pandas as pd
import random
import datetime



def create_database(db_name, host='localhost', user='your_username', password='your_password'):
    connection = None
    cursor = None
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(host=host, user=user, password=password)
        cursor = connection.cursor()
        
        # Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        print(f"Database '{db_name}' created successfully or already exists.")
    
    except Error as err:
        print(f"Error: {err}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
def create_connection(db_name, host='localhost', user='your_username', password='your_password'):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        print(f"Connected to the database '{db_name}' successfully.")
        return connection
    except Error as err:
        print(f"Error: {err}")
        return None
    

# Function to create tables
def create_tables(cursor):
    # Define SQL commands for creating the tables
    create_products_table = '''CREATE TABLE IF NOT EXISTS products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(50),
        unit_cost DECIMAL(10, 2)
    )'''
    
    create_customers_table = '''CREATE TABLE IF NOT EXISTS customers (
        customer_id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(15)
    )'''
    
    create_sales_table = '''CREATE TABLE IF NOT EXISTS sales (
        sale_id INT AUTO_INCREMENT PRIMARY KEY,
        sale_date DATE,
        customer_id INT,
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10, 2),
        total_price DECIMAL(10, 2),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )'''
    
    # Create the tables
    cursor.execute(create_products_table)
    cursor.execute(create_customers_table)
    cursor.execute(create_sales_table)
    print("Tables created successfully.")
    
# Function to insert sample data into tables
def insert_sample_data(cursor):
    # Define sample data for the products and customers tables
    products = [('Product A', 50.00), ('Product B', 25.00), ('Product C', 75.00), ('Product D', 40.00), ('Product E', 60.00)]

    customers = [
        ('John', 'Doe', 'johndoe@example.com', '555-1234'),
        ('Jane', 'Doe', 'janedoe@example.com', '555-5678'),
        ('Bob', 'Smith', 'bobsmith@example.com', '555-9012'),
        ('Alice', 'Jones', 'alicejones@example.com', '555-3456'),
        ('David', 'Brown', 'davidbrown@example.com', '555-7890'),
        ('Emily', 'Davis', 'emilydavis@example.com', '555-2345'),
        ('Frank', 'Wilson', 'frankwilson@example.com', '555-6789'),
        ('Grace', 'Lee', 'gracelee@example.com', '555-1234'),
        ('Henry', 'Chen', 'henrychen@example.com', '555-5678'),
        ('Isabel', 'Garcia', 'isabelgarcia@example.com', '555-9012')
    ]

    # Define SQL commands for inserting sample data into the tables
    insert_products_data = '''INSERT INTO products (product_name, unit_cost) VALUES (%s, %s)'''
    insert_customers_data = '''INSERT INTO customers (first_name, last_name, email, phone) VALUES (%s, %s, %s, %s)'''

    # Insert sample data into the products and customers tables
    for product in products:
        cursor.execute(insert_products_data, product)

    for customer in customers:
        cursor.execute(insert_customers_data, customer)

    print("Sample data inserted successfully.")
    
    # Return the list of products so it can be used in the sales data function
    return products

# Function to insert sales data
def insert_sales_data(cursor, start_date, end_date, products):
    """
    Insert randomly generated sales data into the 'sales' table.

    Args:
        cursor: A cursor object connected to the MySQL database.
        start_date: The starting date for generating random sale dates.
        end_date: The ending date for generating random sale dates.
        products: A list of products available, each product represented as a tuple (product_name, unit_cost).
    """
    # Define SQL command for inserting sales data
    insert_sales_data_query = '''INSERT INTO sales (sale_date, customer_id, product_id, quantity, unit_price, total_price) VALUES (%s, %s, %s, %s, %s, %s)'''

    # Insert sales data into the sales table
    for i in range(1000):
        # Generate random sales data
        sale_date = start_date + datetime.timedelta(days=random.randint(0, 364))
        customer_id = random.randint(1, 10)
        product_id = random.randint(1, len(products))
        quantity = random.randint(1, 10)
        
        # Get the unit price from the products list
        unit_price = products[product_id - 1][1]
        
        # Calculate total price
        total_price = quantity * unit_price
        
        # Insert the sales data
        try:
            cursor.execute(insert_sales_data_query, (sale_date, customer_id, product_id, quantity, unit_price, total_price))
        except mysql.connector.Error as err:
            print(f"Error inserting sales data: {err}")
            continue

    print("Sales data inserted successfully.")
    
# Main script
def main():
    db_name = 'sales_data'
    host = 'localhost'
    user = 'root'
    password = 'Combination2#'

    # Create the database
    create_database(db_name, host, user, password)

    # Create a connection to the MySQL database
    connection = create_connection(db_name, host, user, password)

    if connection:
        cursor = connection.cursor()

        # Create tables
        create_tables(cursor)

        # Insert sample data
        products = insert_sample_data(cursor)

        # Define the start and end dates for generating sales data
        start_date = datetime.date(2022, 1, 1)
        end_date = datetime.date(2022, 12, 31)

        # Insert sales data
        insert_sales_data(cursor, start_date, end_date,products)

        # Commit the changes
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        print("Data has been inserted successfully into the database.")

if __name__ == '__main__':
    main()
