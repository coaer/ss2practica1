#Las librerias necesarias
#import pyodbc
import mysql.connector
from mysql.connector import Error
import pandas as pd

from imprimir import *
from creacion import *
from consultas import *
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
    connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')
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
            crearBD()            
        elif opcion=='2':            
            logger.info('USUARIO REALIZA OPERACION DE ETL')
            crearModelo()
        elif opcion=='3':
            logger.info('USUARIO REALIZA OPERACION DE CONSULTAS')
            ejecutar_consulta(1)
        else:
            connection.close()
            logger.info('Conexion finalizada')
            exit()

def crearBD():
    try:
        logger.info(f'INICIANDO CREACION DE MODELO DE BASE DE DATOS')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')        
        cursor = connection.cursor()
        with open('bd.sql', encoding="utf-8") as f:
            commands = f.read().split(';')
        for command in commands:
            cursor.execute(command)           
        #connection.commit()
        logger.info(f'BASE DE DATOS CREADA EXITOSAMENTE') 
        cursor.close()
    except mysql.connector.Error as error:
        print("ERROR AL CREAR LA BASE DE DATOS {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()


def crearSP():
    try:
        logger.info(f'INICIA CREACIÓN DE SP')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')        
        cursor = connection.cursor()
        with open('sp.sql', encoding="utf-8") as f:
            commands = f.read().split(';')
        for command in commands:
            cursor.execute(command)           
        connection.commit()
        logger.info(f'TERMINA CREACIÓN DE SP') 
        cursor.close()
    except mysql.connector.Error as error:
        print("ERROR AL CREAR EL SP {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()

          
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
        data = pd.read_csv("songs_normalize.csv",encoding='utf-8')
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
            else:
                i = i +1
            artist = str(row[1].replace("'","''"))
            song = str(row[2].replace("'","''"))
            genre = str(row[18].replace("'","''"))
            query = (f'INSERT INTO tmp VALUES(\'{artist}\',\'{song}\',{row[3]},\'{row[4]}\',{row[5]},{row[6]},{row[7]},{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]},{row[15]},{row[16]},{row[17]},\'{genre}\')')            
            logger.info(query)            
            #print(query)

            cursor.execute(query)
            #connection.execute(query)
        #logger(cursor.execute("SELECT * FROM temporal LIMIT 1"))
        logger.info(f'Se insertaron correctamente {i-1} filas')
    except Error as e:
        logger.info("ERROR AL INSERTAR LA FILA : "+ str(e))
    finally:
        #logger(cursor.execute("SELECT * FROM temporal LIMIT 1"))        
        connection.commit()
        carga_generos_dw()
        carga_artistas_dw()
        carga_canciones_dw()

def carga_generos_dw():
    try:
        logger.info(f'INICIANDO CARGA DE GENEROS EN DATA WAREAHOUSE')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')
        mySql_insert_query = "INSERT INTO genero(nombre) select distinct genre from tmp where genre<>'set()'"
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info(f'GENEROS INSERTADOS ' +str(cursor.rowcount)) 
        cursor.close()
    except mysql.connector.Error as error:
        print("Failed to insert record into Laptop table {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()            


def carga_artistas_dw():
    try:
        logger.info(f'INICIANDO CARGA DE GENEROS EN DATA WAREAHOUSE')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')
        mySql_insert_query = "INSERT INTO artista(nombre) select distinct artist from tmp"
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info(f'GENEROS INSERTADOS ' +str(cursor.rowcount)) 
        cursor.close()
    except mysql.connector.Error as error:
        print("Failed to insert record into Laptop table {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()            

def carga_canciones_dw():
    try:
        logger.info(f'INICIA CARGA DE CANCIONES')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')        
        cursor = connection.cursor()
        cursor.callproc('sp_carga_canciones_dw')
        connection.commit()
        logger.info(f'FINALIZA CARGA DE CANCIONES') 
        cursor.close()
    except mysql.connector.Error as error:
        print("ERROR CARGAR CANCIONES {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()


def realiza_consultas():
    try:
        logger.info(f'INICIA PROCESO DE CONSULTAS')
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')        
        cursor = connection.cursor()
        cursor.callproc('sp_carga_canciones_dw')
        connection.commit()
        logger.info(f'FINALIZA PROCESO DE CONSULTAS') 
        cursor.close()
    except mysql.connector.Error as error:
        print("ERROR AL CREAR EL SP {}".format(error))

    finally:
        if connection.is_connected():
            connection.close()












def ejecutar_consulta(num):    
    try:
        resultadoConsultas.info("CONSULTA 1: TOP 10 DE ARTISTAS CON MAYORES REPRODUCCIONES")
        #cursor = conn.cursor()
        connection = mysql.connector.connect(host='localhost',database='dwrm',user='root',password='123456')       
        cursor = connection.cursor()     
        result = cursor.execute(QUERY1)    
        for row in result:        
            resultadoConsultas.info(f"{row[0]}")
    except Exception as e:
        logger.error(e)
        connection.close()
        exit()









if __name__ == "__main__":
    main()
    logger.info('Aplicacion finalizada')