import datetime
import mysql.connector
import PyMyHealth

conn = mysql.connector.connect(user='mcotner', password='gr33nday',
                               host='localhost', database='employees',
                               auth_plugin='mysql_native_password')

# TODO: Remove later
now = datetime.time()

health = PyMyHealth.PyMyHealth(conn)
