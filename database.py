import mysql.connector
import mysql
import time

class database():
    def __init__(self):
        self.isConnect = False
        self.dbHost = 'snf-876565.vm.okeanos.grnet.gr'
        self.dbName = 'crawlerdb'
        self.dbUser = 'root'
        self.dbPass = '10dm1@b0320'

    def connect(self):

        try:
            self.connection = mysql.connector.connect(host=self.dbHost, database=self.dbName, user=self.dbUser,password=self.dbPass)
            if self.connection.is_connected():
                self.isConnect = True
        except:
            self.isConnect = False

    def close(self):
        self.connection.close()

    def checkConnection(self):
        return self.isConnect

    def query(self, query):
        if (self.isConnect is False):
            self.connect()
        # print(query)
        cursor = self.connection.cursor()
        self.connection.commit()
        cursor.execute(query)
        records = cursor.fetchone()
        return records[0]

    def backup_query(self, query):
        if (self.isConnect is False):
            self.connect()
        # print(query)
        cursor = self.connection.cursor()
        self.connection.commit()
        cursor.execute(query)
        records = cursor.fetchall()
        return records


    def kariera_insert(self, job_url, job_html,sql):
        if (self.isConnect is False):
            self.connect()
        try:
            self.cursor = self.connection.cursor(prepared=True)
            date = time.strftime('%Y-%m-%d')
            insert_data = (str(job_url), str(job_html), date)
            self.cursor.execute(sql, insert_data)
            cursor = self.connection.cursor()
            self.connection.commit()
        except mysql.connector.Error as error:
            self.connection.rollback()
            print("Failed to insert into MySQL table {}".format(error))

    def kariera_list(self,sql):
        if (self.isConnect is False):
            self.connect()
        try:
            self.cursor = self.connection.cursor(prepared=True)
            self.cursor.execute(sql)
            records = self.cursor.fetchall()  # this is dangerous should be replaced!!!!
            self.records = records
            return self.records
        except mysql.connector.Error as error:
            self.connection.rollback()
            print("Failed to connect {}".format(error))









