import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min
from pyspark.sql import functions as sf
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import rank
from pyspark.sql.window import Window
from pyspark.sql.types import (IntegerType, StringType, FloatType, StructField,
                               StructType, TimestampType)

spark = SparkSession.builder.appName("Leer Estudiantes").getOrCreate()

####################################
# Leyendo Datos de Estudiantes
####################################

def genera_data_frame_estudiantes(csv_estudiantes):

	estudiantes_schema = StructType([StructField('carnet', IntegerType()),
        	             StructField('nombre', StringType()),
	                     StructField('carrera', StringType())])

	_df_estudiantes = spark.read.csv(csv_estudiantes,
	                           schema=estudiantes_schema,
	                           header=False)
	return _df_estudiantes

####################################
# Leyendo Datos de Notas
####################################

def genera_data_frame_notas(csv_notas):

	notas_schema = StructType([StructField('carnet', IntegerType()),
     		       StructField('codigo_curso', IntegerType()),
                       StructField('Nota', IntegerType())])
 
	_df_notas = spark.read.csv(csv_notas,
                                  schema=notas_schema,
                                  header=False)
	return _df_notas



####################################
# Leyendo Datos de Curso
####################################

def genera_data_frame_cursos(csv_cursos):

	curso_schema = StructType([StructField('codigo_curso', IntegerType()),
	               StructField('creditos', IntegerType()),
	               StructField('carrera', StringType())])

	_df_cursos = spark.read.csv(csv_cursos,
                           schema=curso_schema,
                           header=False)
	return _df_cursos

#######################################################################
# Haciendo join de las tablas disponibles
#######################################################################

def join_cursos_notas(_df_cursos,_df_notas):

	_cursos_notas_df = _df_notas.join(_df_cursos, "codigo_curso") 
	_cursos_notas_df = _cursos_notas_df.select("carnet","codigo_curso","creditos","Nota","carrera")                                                  	
	return _cursos_notas_df 

def join_notas_estudiantes(_cursos_notas_df,_df_estudiantes):
	
	_nombre_cursos_df = _cursos_notas_df.join(_df_estudiantes, "carnet").drop(_cursos_notas_df.carrera)
	_nombre_cursos_df = _nombre_cursos_df.select("carnet","nombre","codigo_curso","creditos","Nota","carrera");
	
	return _nombre_cursos_df

def calcula_total_creditos_estudiante(_nombre_cursos_df):

	_df_total_creditos_estudiante = _nombre_cursos_df.withColumn("total_con_creditos",(col('Nota')*col('creditos')))
	_df_total_creditos_estudiante = _df_total_creditos_estudiante.groupBy("carnet","nombre","carrera")\
		                                                   .agg(sf.sum("creditos").alias("creditos_cursados"),                                    
                                                                        sf.sum("total_con_creditos").alias("notas_sumadas")) 
	return _df_total_creditos_estudiante

def calcula_promedio_estudiantes(_df_total_creditos_estudiante):
	_df_promedio_estudiantes = _df_total_creditos_estudiante.withColumn("Promedio",(col('notas_sumadas')/col('creditos_cursados')))
	_df_promedio_estudiantes = _df_promedio_estudiantes.orderBy(sf.desc("Promedio"))
	return _df_promedio_estudiantes

def calcula_mejor_promedio_carrera(_df_promedio_estudiantes):

	_df_mejor_promedio_carrera = _df_promedio_estudiantes.groupBy("carrera")\
		               .agg(sf.max("Promedio").alias("Promedio"),\
                	            sf.first("carnet").alias("carnet"),\
                                    sf.first("nombre").alias("nombre"))
 
	_df_mejor_promedio_carrera = _df_mejor_promedio_carrera.orderBy(sf.desc("Promedio"))
	return _df_mejor_promedio_carrera

def calcula_n_mejores_promedios_carrera(_df_promedio_estudiantes,n):

	windowSpec = Window.partitionBy("carrera").orderBy(sf.desc("Promedio"))
	_df_mejor_n_promedios = _df_promedio_estudiantes.select('*',rank().over(windowSpec).alias('rank')).filter(col('rank') <= n)
	_df_mejor_n_promedios = _df_mejor_n_promedios.select("carrera","carnet","nombre","Promedio","rank")
	return _df_mejor_n_promedios

#################################################################################

#####Leyendo archivos csv provenientes de la linea de comando ########

x = len(sys.argv)

if(x >= 3):
	estudiante_csv = sys.argv[1]
	curso_csv = sys.argv[2]
	nota_csv = sys.argv[3]
else:
	estudiante_csv = "estudiante.csv"
	curso_csv = "curso.csv"
	nota_csv = "nota.csv"

############ Iniciando programa #######################################

df_estudiantes = genera_data_frame_estudiantes(estudiante_csv)

df_notas = genera_data_frame_notas(nota_csv)

df_cursos = genera_data_frame_cursos(curso_csv)

cursos_notas_df = join_cursos_notas(df_cursos,df_notas)

nombre_cursos_df = join_notas_estudiantes(cursos_notas_df,df_estudiantes)

df_total_creditos_estudiante = calcula_total_creditos_estudiante(nombre_cursos_df)

df_promedio_estudiantes = calcula_promedio_estudiantes(df_total_creditos_estudiante)

n=2
df_mejor_n_promedios = calcula_n_mejores_promedios_carrera(df_promedio_estudiantes,n)
df_mejor_n_promedios.show(df_mejor_n_promedios.count())  
