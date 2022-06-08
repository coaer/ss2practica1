#Las librerias necesarias
#import pyodbc
import mysql.connector
from mysql.connector import Error
import pandas as pd

from imprimir import *
from creacion import *
import config
import logging

#Configurar nuestro log

logger = logging.getLogger('PRACTICA 1 SEMINARIO DE SISTEMAS 1 -201213194')
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('logs.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#Configurar nuestra conexion
try:
    logger.info("INICIANDO CONEXION A MYSQL SERVER ")
    connection = mysql.connector.connect(host='localhost',database='prueba',user='root',password='123456')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        logger.info("Conectado a MySQL Server version "+db_Info)        
        cursor = connection.cursor()
        logger.info("Realiza la creación de cursor ")        
except Error as e:
    logger.info("Error al conectarse a MySQL : "+ str(e))
#finally:
    #if connection.is_connected():
        #cursor.close()
        #connection.close()
        #print("MySQL")


def main():
    menu()
    #print("Hola mundo")

def menu():
    while True:
        print_main_menu()
        opcion = input('Elija la operación que desea: ')
        if opcion=='1':
            logger.info("USUARIO REALIZA OPERACION DE CREAR MODELO")
            crearModelo()
        elif opcion=='2':
            logger.info('USUARIO REALIZA OPERACION DE CARGA')
        elif opcion=='3':
            logger.info('USUARIO REALIZA OPERACION DE CONSULTAS')
        else:
            connection.close()
            logger.info('Conexion finalizada')
            exit()
            
def crearModelo():
    logger.info("OPERACION DE CREAR TABLAS...")
    logger.info("ELIMINANDO TABLAS...")
    #cursor = conn.cursor()
    cursor.execute(DROP_TABLES)
    logger.info("Tablas eliminadas correctamente")
    logger.info("Creando las tablas necesarias")
    logger.info("Creando tabla temporal")
    cursor.execute(TEMPORAL_CREATION)
    logger.info("Comenzando a procesar el dataset")
    try:
        data = pd.read_csv("songs_normalize.csv")
        df = pd.DataFrame(data)
        logger.info("Dataset leido exitosamente")
        cargar_temporal(df)
    except Exception as e:
        logger.error(e)
        connection.close()
        exit()

def cargar_temporal(df):
    try:
        cursor = connection.cursor()
        i = 0
        for row in df.itertuples():
            if(i==0):
                i = i +1
                continue
            artist = str(row[1].replace("'","''"))
            song = str(row[2].replace("'","''"))
            genre = str(row[18].replace("'","''"))
            query = (f'INSERT INTO temporal VALUES(\'{artist}\',\'{song}\',{row[3]},\'{row[4]}\',{row[5]},{row[6]},{row[7]},{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]},{row[15]},{row[16]},{row[17]},\'{genre}\')')
            logger.info(query)
            #print(query)

            cursor.execute(query)
            #connection.execute(query)
        #logger(cursor.execute("SELECT * FROM temporal LIMIT 1"))
        logger.info(f'Se insertaron correctamente {i} filas')
    except Error as e:
        logger.info("ERROR AL INSERTAR LA FILA : "+ str(e))
    finally:
        #logger(cursor.execute("SELECT * FROM temporal LIMIT 1"))
        connection.commit()


if __name__ == "__main__":
    main()
    logger.info('Aplicacion finalizada')