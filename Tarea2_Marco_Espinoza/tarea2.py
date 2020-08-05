from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, ArrayType, DoubleType
import sys,os,json
sc = SparkContext()

#from pyspark import SparkFiles 
spark = SparkSession.builder.appName("Basic Read and Print").getOrCreate()

def loading_json_files():
	schema = StructType([StructField("compras", ArrayType(ArrayType(StructType([
                             StructField("cantidad",StringType()),
                             StructField("nombre",StringType()),
                             StructField("precio_unitario",StringType())])))),
                             StructField("numero_caja",StringType())])

###############################################################################################################
# Loading Files
###############################################################################################################

	path_to_json = './'
	json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

	print(json_files)  
	len_json = len(json_files)

	python_file = ""

	for i in range(0,len_json):
		data = open(json_files[i],"r").read()
		python_json = json.loads(data)
		if(i==0):
			_df_json = spark.read.schema(schema).json(sc.parallelize([python_json]))
		else:
			_df_json = _df_json.union(spark.read.schema(schema).json(sc.parallelize([python_json])))

	return _df_json

def exploding_data_frame(_dataframe):
	_dataframe = _dataframe.select("numero_caja",sf.explode("compras"))
	_dataframe = _dataframe.select("numero_caja",sf.explode("col"))
	return _dataframe

def get_total_precio_data(_dataframe):
	_dataframe = _dataframe.withColumn("Cantidad",sf.col("col.cantidad"))
	_dataframe = _dataframe.withColumn("Nombre",sf.col("col.nombre"))
	_dataframe = _dataframe.withColumn("Precio Unitario",sf.col("col.precio_unitario"))
	_dataframe = _dataframe.select("numero_caja","Cantidad","Nombre","Precio Unitario");
	_dataframe = _dataframe.withColumn("total_precio_por_producto",(sf.col('cantidad')*sf.col('Precio Unitario')).cast(IntegerType()))

	return _dataframe

def get_total_productos_table(_dataframe):
	_total_productos = _dataframe.select('Nombre','Cantidad')
	_total_productos = _total_productos.groupBy("Nombre").agg(sf.sum("Cantidad").alias("Total"))
	_total_productos = _total_productos.orderBy(sf.desc("Total"))
	_total_productos = _total_productos.withColumn("Total",sf.col("Total").cast(IntegerType()))
	_total_productos.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("total_productos")
	os.system("cp total_productos/*.csv total_productos.csv")
	os.system("rm -rf total_productos")


	return _total_productos

def get_total_cajas_table(_dataframe):
	_total_cajas = _dataframe.select("numero_caja","total_precio_por_producto")
	_total_cajas = _total_cajas.groupBy("numero_caja").agg(sf.sum("total_precio_por_producto").alias("Total Caja"))
	_total_cajas = _total_cajas.orderBy(sf.desc("Total Caja"))
	_total_cajas = _total_cajas.withColumn("Total Caja",sf.col("Total Caja").cast(IntegerType()))

	_total_cajas.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("total_cajas")
	os.system("cp total_cajas/*.csv total_cajas.csv")
	os.system("rm -rf total_cajas")
	return _total_cajas

def caja_mas_ventas(_total_caj):
	_metricas_mas_ventas = _total_caj.orderBy(sf.desc("Total Caja"))
	_metricas_mas_ventas = _metricas_mas_ventas.select("numero_caja")
	_metricas_mas_ventas_str = str(_metricas_mas_ventas.collect()[0]['numero_caja'])
	return _metricas_mas_ventas_str

def caja_menos_ventas(_total_caj):
	_metricas_menos_ventas = _total_caj.orderBy(sf.asc("Total Caja"))
	_metricas_menos_ventas = _metricas_menos_ventas.select("numero_caja")
	_metricas_menos_ventas_str = str(_metricas_menos_ventas.collect()[0]['numero_caja'])
	return _metricas_menos_ventas_str

def producto_mas_vendido(_total_prod):
	_metricas_mas_vendido = _total_prod.orderBy(sf.desc("Total"))
	_metricas_mas_vendido = _metricas_mas_vendido.select("Nombre")
	_metricas_mas_vendido_str = str(_metricas_mas_vendido.collect()[0]['Nombre'])
	return _metricas_mas_vendido_str

def producto_mayor_ingreso(_dataframe):
	_metricas_mayor_ingreso = _dataframe.select('Nombre','Cantidad','total_precio_por_producto')
	_metricas_mayor_ingreso = _metricas_mayor_ingreso.groupBy("Nombre").agg(sf.sum("total_precio_por_producto").alias("Total Producto"))
	_metricas_mayor_ingreso = _metricas_mayor_ingreso.orderBy(sf.desc("Total Producto"))
	_metricas_mayor_ingreso = _metricas_mayor_ingreso.select("Nombre")
	_metricas_mayor_ingreso_str = str(_metricas_mayor_ingreso.collect()[0]['Nombre'])
	return _metricas_mayor_ingreso_str

def percentiles_caja(_total_caj):

	_metricas_perc_caja = _total_caj.orderBy(sf.asc("Total Caja"))
	_metricas_perc_caja = _metricas_perc_caja.select("numero_caja")

	p25 = int(0.25*_metricas_perc_caja.count())
	p50 = int(0.50*_metricas_perc_caja.count())
	p75 = int(0.75*_metricas_perc_caja.count())

	_metricas_p25_caja_str = str(_metricas_perc_caja.collect()[p25]['numero_caja'])
	_metricas_p50_caja_str = str(_metricas_perc_caja.collect()[p50]['numero_caja'])
	_metricas_p75_caja_str = str(_metricas_perc_caja.collect()[p75]['numero_caja'])

	_metricas_px = (_metricas_p25_caja_str,_metricas_p50_caja_str,_metricas_p75_caja_str)

	return _metricas_px

def get_metricas_table(_dataframe,_total_prod,_total_caj,_metricas_mas_ventas_str,_metricas_menos_ventas_str,_metricas_mas_vendido_str,_metricas_mayor_ingreso_str,str_metricas):
	
	_metricas_p25_caja_str = str_metricas[0]
	_metricas_p50_caja_str = str_metricas[1]
	_metricas_p75_caja_str = str_metricas[2]


	_metricas_data = [("caja_mas_ventas",_metricas_mas_ventas_str),
	                  ("caja_menos_ventas",_metricas_menos_ventas_str),
	                  ("percen_25_cajas",_metricas_p25_caja_str),
	                  ("percen_50_cajas",_metricas_p50_caja_str),
	                  ("percen_75_cajas",_metricas_p75_caja_str),
	                  ("prod_mas_vendido",_metricas_mas_vendido_str),
	                  ("prod_mayor_ingreso",_metricas_mayor_ingreso_str)]

	_metricas = spark.createDataFrame(_metricas_data,['Metricas','Valor'])
	_metricas.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("metricas")
	os.system("cp metricas/*.csv metricas.csv")
	os.system("rm -rf metricas")

#### Program Main ##########################

_df_from_json = loading_json_files();
_df_exploded = exploding_data_frame(_df_from_json)
_df_all_data = get_total_precio_data(_df_exploded)
_total_producto = get_total_productos_table(_df_all_data)
_total_caja = get_total_cajas_table(_df_all_data)

str_caja_mas_ventas = caja_mas_ventas(_total_caja)
str_caja_menos_ventas = caja_menos_ventas(_total_caja)
str_prod_mas_vendido = producto_mas_vendido(_total_producto)
str_prod_mas_ingreso = producto_mayor_ingreso(_df_all_data)
str_metricas_px = percentiles_caja(_total_caja)

get_metricas_table(_df_all_data,_total_producto,_total_caja,str_caja_mas_ventas,str_caja_menos_ventas,str_prod_mas_vendido,str_prod_mas_ingreso,str_metricas_px)




