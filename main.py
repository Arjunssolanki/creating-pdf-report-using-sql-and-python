import mysql.connector as connector

class DBhelper:
    def __init__(self):
        self.con = connector.connect(
            host='localhost',
            port='3306',
            user='root',
            password='Combination2#',
            database='data'
            )
        query ='create table if not exists user(userId int primary key,username varchar(200),phone varchar(12))'
        cursor = self.con.cursor()
        cursor.execute(query)
        print("created")
        
    def insert_data(self,userid,username,phone):
        query = """ insert IGNORE into user(userid,username,phone)
        values({},'{}','{}')""".format(userid,username,phone) 
        print(query)
        cursor = self.con.cursor()
        cursor.execute(query)
        self.con.commit()
        print("user saved to db")
        
    def fetch_data(self):
        query = """select * from user"""
        cursor = self.con.cursor()
        cursor.execute(query)
        print("fetching data")
        for i in cursor:
            print("userid: ",i[0])
            print("username: ",i[1])
            print("phone: ",i[2])
            print()
            print()
            
    def delete_data(self,userid):
        print("deleating data")
        query = "delete from user where userid={}".format(userid)
        cursor = self.con.cursor()
        cursor.execute(query)
        self.con.commit() # it will make permanent changes

# creating table
helper = DBhelper()
helper.insert_data(1234,"arjun",'343434')
helper.insert_data(12345,"arn",'3434343')
helper.insert_data(12346,"arun",'3434534')
helper.insert_data(12347,"rjun",'3434364')
helper.fetch_data()
helper.delete_data(12347)
helper.fetch_data()
print("deleted")

