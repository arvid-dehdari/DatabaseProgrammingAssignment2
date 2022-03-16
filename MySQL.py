# Amirreza (Arvid) Dehdari 
# Rashad Abbas 
# track all the activities of the clients regarding the balance, payment methods, invoice date, and due date.

import os
import csv
import mysql.connector as mysq
from datetime import datetime

# my_Database = mysq.connect(host="127.0.0.1", user="root", password="root")
# Database = "invoice_database"


# connect to my database 
my_Database = mysq.connect(host="127.0.0.1", user="root", password="root")
mycursor = my_Database.cursor()

# check if the database with a given name allready exists.
# since thier is no database a database invoice_database will
# be created later first time the program run .


def isDbExists(name):
    mycursor = my_Database.cursor()
    mycursor.execute(f"SHOW DATABASES LIKE '{name}'")
    result = mycursor.fetchone()
    if result:
        return True
    else:
        return False

# check if the table with a given name already exists

def isTableExists(name):
    mycursor = my_Database.cursor()
    mycursor.execute(f"SHOW TABLES LIKE '{name}'")
    result = mycursor.fetchone()
    if result:
        return True
    else:
        return False

# check if the table are empty 

def isTableEmpty(name):
    mycursor = my_Database.cursor()
    mycursor.execute(f"SELECT * FROM {name} LIMIT 1")
    result = mycursor.fetchone()
    if not result:
        return True
    else:
        return False

# If the database does not exists so create a database 


if not isDbExists("invoice_database"):
    print("Create database")
    mycursor = my_Database.cursor()
    mycursor.execute("CREATE DATABASE invoice_database")

my_Database = mysq.connect(
    host="127.0.0.1", user="root", password="root", database="invoice_database")
mycursor = my_Database.cursor()

# if their is no table create table clients
# first time the program run the table will be created
if not isTableExists("clients"):
    print("Creating table clients..")
    mycursor.execute("CREATE TABLE `clients` (`client_id` int(11) NOT NULL,\
   `name` varchar(50) NOT NULL,\
   `address` varchar(50) NOT NULL,\
   `city` varchar(50) NOT NULL,\
   `state` char(2) NOT NULL,\
   `phone` varchar(50) DEFAULT NULL,\
    PRIMARY KEY (`client_id`))")
# if their is no table create table payments
# first time the program run the table will be created
if not isTableExists("payments"):
    print("Creating table payments..")
    mycursor.execute("CREATE TABLE `payments` (`payment_id` int ,\
    `client_id` int ,\
    `invoice_id` int ,\
    `date` date ,\
    `amount` decimal(9,2) ,\
    `payment_method` int,\
    PRIMARY KEY (`payment_id`),\
    KEY `fk_client_id_idx` (`client_id`),\
    KEY `fk_invoice_id_idx` (`invoice_id`),\
    KEY `fk_payment_payment_method_idx` (`payment_method`))")

# if their is no table create table invoices
# first time the program run the table will be created
if not isTableExists("invoices"):
    print("Creating table invoices..")
    mycursor.execute("CREATE TABLE `invoices` (`invoice_id` int(11) NOT NULL,\
    `number` varchar(50) NOT NULL,\
    `client_id` int(11) NOT NULL,\
    `invoice_total` decimal(9,2) NOT NULL,\
    `payment_total` decimal(9,2) NOT NULL DEFAULT '0.00',\
    `invoice_date` date NOT NULL,\
    `due_date` date NOT NULL,\
    `payment_date` date DEFAULT NULL,\
    PRIMARY KEY (`invoice_id`),\
    KEY `FK_client_id` (`client_id`),\
    CONSTRAINT `FK_client_id` FOREIGN KEY (`client_id`) REFERENCES `clients` (`client_id`) ON DELETE RESTRICT ON UPDATE CASCADE)")

# if their is no table create table payment_methods
# first time the program run the table will be created
if not isTableExists("payment_methods"):
    print("Creating table payment_methods..")
    mycursor.execute("CREATE TABLE `payment_methods` (`payment_method_id` int(11) NOT NULL,\
    `name` varchar(50) NOT NULL,\
    PRIMARY KEY (`payment_method_id`))")



# add data to the table clients from the csv file
if isTableEmpty("clients"):
    print("Table clients is empty, importing data...")

    file = open("clients.csv")
    csvreader = csv.reader(file)
    # skip the headers
    next(csvreader)

    for row in csvreader:

        sql = "INSERT INTO invoice_database.clients VALUES (%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        my_Database.commit()


# add data to the table payment_methods from the csv file
if isTableEmpty("payment_methods"):
    print("Table payment_methods is empty, importing data...")
    file = open("method.csv")
    csvreader = csv.reader(file)
    # skip the headers
    next(csvreader)

    for row in csvreader:

        sql = "INSERT INTO invoice_database.payment_methods VALUES (%s,%s)"
        mycursor.execute(sql, tuple(row))
        my_Database.commit()

# add data to the table payments from the csv file
if isTableEmpty("payments"):
    print("Table payments is empty, importing data...")
    file = open("payments.csv")
    csvreader = csv.reader(file)
    # skip the headers
    next(csvreader)

    for row in csvreader:
        tempRow = row
        
        tempRow[3] = datetime.strptime(row[3], "%Y-%m-%d")
        tempRow[4] = float(row[4])

        sql = "INSERT INTO invoice_database.payments VALUES (%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(tempRow))
        my_Database.commit()

# add data to the table invoices from the csv file
if isTableEmpty("invoices"):
    print("Table invoices is empty, importing data...")
    file = open("invoices.csv")
    csvreader = csv.reader(file)
    
    # skip the headers
    next(csvreader)

    for row in csvreader:
        tempRow = row
        # fix the date format 
        tempRow[5] = datetime.strptime(row[5], "%Y-%m-%d")
        tempRow[6] = datetime.strptime(row[6], "%Y-%m-%d")
        if tempRow[7] != "NULL":
            tempRow[7] = datetime.strptime(row[7], "%Y-%m-%d")
        else: 
            tempRow[7] = None
    
        sql = "INSERT INTO invoice_database.invoices VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(tempRow))
        my_Database.commit()

# main menu
# press any key to return to main menu
# press q or Q to quiet 
# program start  
while (1):
    print("Main Menu")
    print("Select one of the Options : ")
    print("[1] List the name of all the clients")
    print("[2] Search for a cleints details")
    print("[3] Show amount of payments from the clients for every day and the payment method")
    print("[4] Show a money report for FIRST HALF OF 2019  and SECOND HALF OF 2019  ")
    print("[5] Show the total balance of each client and also the sum of all balances")
    print("[6] Create view for the total balance of each client and the sum of all balances")
    print("[7] Search for client total balance ")
    print("[8] Search  for an Individual report for any client")
    print("[9] Classify the clients by Gold and silver")
    print("[10] Search for information based on the invoice number")

    print("Enter Q to quit, or press return to continue ")
    user_input = input(" Choose from 1 to 10 Enter Q or q to quit : ")
    # Quit
    if user_input.isalpha():
        if user_input == "Q" or user_input == "q":

            break
        continue
    user_input = int(user_input)


    if (user_input == 1):
        # select name from the table clients
        print()

        print("Press any key to return to main menu")    
        print()
        mycursor.execute("select name from clients")
        # fetchall() method which fetches all rows from the last executed
        # statement.
        myresult = mycursor.fetchall()
        for x in myresult:
            print(x[0])
        print()
        print("Press any key to return to main menu")
        os.system("Pause > nul")
  
    elif user_input == 2:
        print()
        client_Name = input("Enter the client Name: \t")
        print()
       
        # select all the information for a specific client
        mycursor.execute(f"select * from clients where name ='{client_Name}'")
        ans = mycursor.fetchall()
        if ans:
            for x in ans:
                print("***** client details *****")
                print(f"client_Id:              {x[0]}")
                print(f"name:                   {x[1]}")
                print(f"address:                {x[2]}")
                print(f"city:                   {x[3]}")
                print(f"state:                  {x[4]}")
                print(f"Phone:                  {x[5]}")
        print()
        print("Press any key to return to main menu")    
        os.system("Pause > nul")


    elif user_input == 3:
            # Show amount of payments for every day
            query = "SELECT date,\
	        name AS payment_method, sum(amount) as total_payment FROM payments p \
            JOIN payment_methods pm ON p.payment_method = pm.payment_method_id GROUP BY date    "
            mycursor.execute(query)
            ans = mycursor.fetchall()
            formatted_row = '{:<25} {:<20} {:<20}'
            print(formatted_row.format("Date", "payment_method" , "total_payment"))
            for x in ans:
                formattedDate = datetime.strftime(x[0], "%Y-%m-%d")

                print(formatted_row.format(formattedDate, x[1], x[2]))
 
            print()

            print("Press any key to return to main menu")    
            os.system("Pause > nul")

    elif user_input == 4:
            # Show a money report for FIRST HALF OF 2019  and SECOND HALF OF 2019
            query = "SELECT 'FIRST HALF OF 2019' AS date_range,\
                    sum(invoice_total) AS total_sales,\
                    sum(payment_total) AS total_payment,\
                    (sum(invoice_total) - sum(payment_total)) AS what_we_expect\
            FROM invoices\
            WHERE invoice_date \
                BETWEEN '2019-01-01' AND '2019-06-30'\
            UNION\
            SELECT 'SECOND HALF OF 2019' AS date_range,\
                    sum(invoice_total) AS total_sales,\
                    sum(payment_total) AS total_payment,\
                    (sum(invoice_total) - sum(payment_total)) AS what_we_expect\
            FROM invoices\
            WHERE invoice_date \
                BETWEEN '2019-07-01' AND '2019-12-31'\
            UNION\
            SELECT 'TOTAL' AS date_range,\
                    sum(invoice_total) AS total_sales,\
                    sum(payment_total) AS total_payment,\
                    (sum(invoice_total) - sum(payment_total)) AS what_we_expect\
            FROM invoices\
            WHERE invoice_date \
                BETWEEN '2019-01-01' AND '2019-12-31'"
                
            mycursor.execute(query)
            ans = mycursor.fetchall()
            formatted_row = '{:<30} {:<20} {:<20} {:<20}'
            print(formatted_row.format("date_range", "total_sales" , "total_payment" , "what_we_expect"))

            for x in ans:
            
                print(formatted_row.format(x[0], str(x[1]),str( x[2]) , str(x[3])))

            print()

            print("Press any key to return to main menu")    
            os.system("Pause > nul")    
    
    elif (user_input == 5):
        # Show the total balance of each client and the sum of all balances
        mycursor.execute("SELECT client_id,\
            c.name,\
            c.address,\
            c.phone,\
        SUM(invoice_total - payment_total) AS total_we_expect\
        FROM invoices i\
        JOIN clients c\
        USING(client_id)\
        GROUP BY client_id\
        HAVING total_we_expect > 0\
        UNION\
        SELECT NULL, NULL, NULL,NULL, sum(invoice_total - payment_total)\
        FROM invoices")

        formatted_row = '{:<10} {:<20} {:<25} {:<20}{:<20}  '
        print(formatted_row.format("client_id", "name" , "address" , "phone", "total_we_wxpect"))
        myresult = mycursor.fetchall()
        for x in myresult:
            tempX = list(x)
            i = 0
            for element in tempX:
                if element == None:
                    tempX[i] = "NULL"
                i += 1
            print(formatted_row.format(tempX[0], tempX[1], tempX[2] , tempX[3], tempX[4]))
        print()

        print("Press any key to return to main menu")
        os.system("Pause > nul")

    elif (user_input == 6):
        # Create view for the total balance of each client and the sum of all balances
        query=("CREATE OR REPLACE VIEW customer_balance_view  AS\
            SELECT client_id,\
                c.name,\
                c.address,\
                c.phone,\
            SUM(invoice_total - payment_total) AS total_we_expect\
            FROM invoices i\
            JOIN clients c\
            USING(client_id)\
            GROUP BY client_id\
            HAVING total_we_expect > 0\
            UNION\
            SELECT NULL, NULL, NULL,NULL, sum(invoice_total - payment_total)\
            FROM invoices")
        print(" View has been created successfully ...")
        mycursor.execute(query)
        ans = mycursor.fetchall()   

        print()

        print("Press any key to return to main menu")
        os.system("Pause > nul")

    

        mycursor.execute(query)
        ans = mycursor.fetchall()       
    elif (user_input == 7):
        print()

        spieciesName = input(" Enter Client name: \t")
        print()

        # Search for client total balance
        query= "SELECT * FROM customer_balance_view \
                    WHERE name ='%s'"%(spieciesName)
        mycursor.execute(query)
        formatted_row = '{:<30} {:<20} {:<25} {:<20}{:<20}  '
        print(formatted_row.format("client_id", "name" , "address" , "phone", "total_we_wxpect"))
        myresult = mycursor.fetchall()
        for x in myresult:
            tempX = list(x)
            print(formatted_row.format(tempX[0], tempX[1], tempX[2] , tempX[3], tempX[4]))

        print()
        print("Press any key to return to main menu")
        os.system("Pause > nul")
    elif user_input == 8:
        print()

        spieciesName = input(" Enter Client name: \t")
        print()
        # Search  for an Individual report for any client
        query="SELECT  SUM(payment_total) AS total_payment,\
            SUM(invoice_total) AS total_invoice,\
            SUM(invoice_total - payment_total) AS balance,\
            COUNT(*) AS total_number_of_invoices \
        FROM invoices i\
        JOIN clients c\
            using(client_id)\
        WHERE invoice_date BETWEEN '2019-01-01' AND '2019-06-30'  AND c.name = '%s'"%(spieciesName)
        mycursor.execute(query)
        ans = mycursor.fetchall()
        formatted_row = '{:<25} {:<20}{:<25} {:<20}'
        print(formatted_row.format("total_payment", "total_invoice", "balance", "total_number_of_invoices"))

        for x in ans:
            print(formatted_row.format(str(x[0]), str(x[1]),str( x[2]) , str(x[3])))
        print()

        print("Press any key to return to main menu")    
        os.system("Pause > nul")    

    elif user_input == 9:
        # Classify the clients
        query="SELECT  'GOLD' AS type,\
                c.name,\
                SUM(amount) AS total_amount \
            FROM payments p\
            JOIN clients c\
                USING(client_id)\
            GROUP BY (client_id)\
            HAVING total_amount > 100\
            UNION\
            SELECT  'SILVER',\
                c.name,\
                SUM(amount) AS total_amount \
            FROM payments p\
            JOIN clients c\
                USING(client_id)\
            GROUP BY (client_id)\
            HAVING total_amount BETWEEN 50 AND 100\
            UNION\
            SELECT  'GOLD' AS type,\
                c.name,\
                SUM(amount) AS total_amount \
            FROM payments p\
            JOIN clients c\
                USING(client_id)\
            GROUP BY (client_id)\
            HAVING total_amount BETWEEN 25 AND 50"
        mycursor.execute(query)
        ans = mycursor.fetchall()
        formatted_row = '{:<25} {:<20}{:<25} '
        print(formatted_row.format("type", "name", "balance"))

        for x in ans:
            print(formatted_row.format(x [0], x[1] , x[2]))
        print()

        print("Press any key to return to main menu")    
        os.system("Pause > nul")

    elif user_input == 10:
        print()

        spieciesNumber = input("Enter the invoice number: \t")
        print()
        # Search for information based on the invoice number
        query="SELECT \
            invoice_date,\
            due_date,\
            payment_date,\
            amount,\
            payment_method\
            FROM payments p \
            JOIN clients c\
                USING(client_id)\
            RIGHT JOIN invoices i\
                USING(invoice_id)\
            WHERE i.number = '%s'"%(spieciesNumber)
            #'91-953-3396' "
        mycursor.execute(query)
        ans = mycursor.fetchall()
        for x in ans:
            tempX = list(x)
            i = 0
            for element in tempX:
                if element == None:
                    tempX[i] = "NULL"
                i += 1
            invoice_date= print("invoice_date :\t" +str(tempX[0]))
            due_date= print("due_date :\t" + str(tempX[1]))
            payment_date= print("payment_date :\t" + str(tempX[2]))
            amount= print("amount :\t" + str(tempX[3]))
            payment_method= print("payment_method :" + str(tempX[4]))
        print()

        print("Press any key to return to main menu")    
        os.system("Pause > nul")

    elif(user_input=="Q"):
        quit()    
 