"""
Created on Sun Sep 20 2025
@author: Group 2 members:
        - Kroch Monyvatana 
        - Seang Kimhak  
        - Heng Vicheka  
        - Khoeurnkosol Roza 

Final Project:
- Topic: Retail Sales Data

ETL Steps:
1. Extract from CSV
2. Transform:
   - rename column
   - cleaning data
   - transform transaction data to customer, product and order
3. Loading data to mysql:
   - create connection, database, tables
   - insert data to db
"""

import pandas as pd
import mysql.connector as conn
import matplotlib as mp
import matplotlib.pyplot as plt

class etl_retail_sale:
    def __init__(self, path):
        # retail_transactions = pd.read_csv(r"D:\TGI\Data-Engineer\exercise\Final-Project\retail_sales_dataset.csv")
        self.retail_transactions = pd.read_csv(path)

    def cleanning(self):
        # Rename Column Name
        self.retail_transactions.rename(columns = {
            "Transaction ID": "transaction_id",
            "Date": "order_date",
            "Customer ID": "customer_id",
            "Gender": "gender",
            "Age": "age",
            "Product Category": "product_category",
            "Quantity": "quantity",
            "Price per Unit": "price_per_unit",
            "Total Amount": "total_amount"
        }, inplace=True)


        # Cleaning Data
        # Check is NA
        print("Count NA: ", str(self.retail_transactions.isna().sum()))

        # Check duplicate
        self.retail_transactions.drop_duplicates(keep="first", inplace=True)
        print("Count Duplicate: "+ str(self.retail_transactions.duplicated().sum()))

        # Apply discount for some products
        discount_rules = {
            "Beauty": 0.10,  # 10% discount
            "Clothing": 0.20,     # 20% discount
            "Electronics": 0.05     # 5% discount
        }

        # Add new column 'discount_amount'
        self.retail_transactions["discount_amount"] = (
            self.retail_transactions["product_category"].map(discount_rules).fillna(0) * self.retail_transactions["total_amount"]
        )

        # Mapping Transaction to Customer
        retail_transactions_customer = self.retail_transactions.drop_duplicates(subset=["customer_id"])
        customer_info = {
            "id": range(1, len(retail_transactions_customer) + 1),
            "customer_code": retail_transactions_customer["customer_id"],
            "name": None, 
            "age": retail_transactions_customer["age"], 
            "gender": self.retail_transactions["gender"], 
            "phone_number": None
        }
        self.customer_data = pd.DataFrame(customer_info)
        print("Total Customer: ", self.customer_data.shape[0])

        # Mapping Transaction to Product
        retail_transactions_product = self.retail_transactions.drop_duplicates(subset=["product_category", "price_per_unit"])
        product_info = {
            "id": range(1, len(retail_transactions_product) + 1),
            "name": retail_transactions_product["product_category"] +"_"+ retail_transactions_product["price_per_unit"].astype(str), 
            "category": retail_transactions_product["product_category"],
            "price": retail_transactions_product["price_per_unit"], 
            "stock": 0, 
            "created_date": None, 
            "expired_date": None
        }
        self.product_data = pd.DataFrame(product_info)
        print("Total Product: ", self.product_data.shape[0])

        # Mapping Transaction to Order
        # self.retail_transactions['order_date'] = pd.to_datetime(self.retail_transactions['order_date'], errors='coerce', infer_datetime_format=True)
        customer_lookup = dict(zip(self.customer_data["customer_code"], self.customer_data["id"]))
        product_lookup = dict(zip(
            zip(retail_transactions_product["product_category"], retail_transactions_product["price_per_unit"]),
            self.product_data["id"]
        ))
        order_Info = {
            "id": self.retail_transactions["transaction_id"],
            "customer_id": self.retail_transactions["customer_id"].map(customer_lookup),
            "product_id": self.retail_transactions.apply(
                lambda x: product_lookup.get((x["product_category"], x["price_per_unit"])), axis=1
            ),
            "quantity": self.retail_transactions["quantity"], 
            "total_amount": self.retail_transactions["total_amount"], 
            "discount_amount": self.retail_transactions["discount_amount"], 
            "revenue": self.retail_transactions["total_amount"] - self.retail_transactions["discount_amount"],
            "order_date": None
        }
        self.order_data = pd.DataFrame(order_Info)
        print("Total Order: ", self.order_data.shape[0])

    def loading(self, db_connection):
        con = conn.connect(
            host= db_connection["host"],
            user= db_connection["username"],
            password= db_connection["password"],
            database= db_connection["database"]
        )
        cursor = con.cursor()

        # Create Database
        cursor.execute("DROP DATABASE IF EXISTS retail_db")
        cursor.execute("CREATE DATABASE retail_db")
        cursor.execute("USE retail_db")

        # Create Customer
        cursor.execute("""
        CREATE 
        TABLE Customer (
            id INT PRIMARY KEY,
            customer_code VARCHAR(50) NOT NULL,
            name VARCHAR(100) NULL,
            age INT,
            gender VARCHAR(100) NULL,
            phone_number VARCHAR(20)
        )"""
        )


        # print(self.product_data.to_string())

        # Create Product
        cursor.execute("""
        CREATE 
        TABLE Product (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(100) NULL,
            price DECIMAL(10,2) NOT NULL,
            stock INT DEFAULT 0,
            created_date DATE,
            expired_date DATE
        )"""
        )

        # Create Order
        cursor.execute(
        """
        CREATE 
        TABLE `Order` (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0, 
                revenue DECIMAL(10,2) NOT NULL DEFAULT 0,
                payment_method VARCHAR(50) NULL,
                order_date DATE,
                FOREIGN KEY (customer_id) REFERENCES Customer(id),
                FOREIGN KEY (product_id) REFERENCES Product(id)
        )
        """
        )

        # Insert Customer
        for _, row in self.customer_data.iterrows():
                cursor.execute(
                """
                INSERT 
                INTO Customer (id, customer_code, name, age, gender, phone_number) 
                VALUES (%s,%s,%s,%s,%s,%s)
                """, tuple(row)
                )

        # Insert Product
        for _, row in self.product_data.iterrows():
                cursor.execute(
                """
                INSERT 
                INTO Product (id, name, category, price, stock, created_date, expired_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, tuple(row)
                )
                
        # Insert Order
        for _, row in self.order_data.iterrows():
                cursor.execute(
                """
                INSERT 
                INTO `Order` (id, customer_id, product_id, quantity, total_amount, discount_amount, revenue, order_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row)
                )

        con.commit()
        cursor.close()
        con.close()

        print("Success ETL Retail Sale Data ! ")

    def plotting(self):
        fig, axes = plt.subplots(1, 1, figsize=(6, 4))
        self.retail_transactions.plot(ax=axes, kind="bar", title="Transaction")
        plt.show()



path = r"D:\TGI\Data-Engineer\exercise\Final-Project\airflow_home\retail_sales_dataset.csv"
db_connection = {
      "host": "localhost",
      "username": "root",
      "password": "1234",
      "database": ""
}

etl  = etl_retail_sale(path)
etl.cleanning()
etl.loading(db_connection)
etl.plotting()














