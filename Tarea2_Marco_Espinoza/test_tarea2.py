import json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, ArrayType, DoubleType
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as sf
from .tarea2 import loading_json_files
from .tarea2 import exploding_data_frame
from .tarea2 import get_total_precio_data
from .tarea2 import get_total_productos_table
from .tarea2 import get_total_cajas_table 
from .tarea2 import get_metricas_table
from .tarea2 import caja_mas_ventas
from .tarea2 import caja_menos_ventas
from .tarea2 import producto_mas_vendido
from .tarea2 import producto_mayor_ingreso
from .tarea2 import percentiles_caja



###Esta prueba inicial verifica que los json files fueron leidos adecuadamente
###y por ello el dataframe obtenido no esta en blanco

def test_loading_json_files(spark_session):
	df_calculado = loading_json_files()
	assert df_calculado.count()!=0

###Prueba para verificar que los datos son convertidos correctamente desde el json file 
###y es convertido a dataframe

def test_exploding_data_frame(spark_session):
	sc = spark_session.sparkContext	
	###Generando string con json format:

	_data_js_string = ['{"numero_caja":"3","compras":[[{"cantidad":"2","nombre":"Harina","precio_unitario":"1500"},\
				   		           {"cantidad":"5","nombre":"Arroz","precio_unitario":"1000"}],\
                                                          [{"cantidad":"4","nombre":"Frijoles","precio_unitario":"800"}],\
                                                          [{"cantidad":"7","nombre":"Manzana","precio_unitario":"500"},\
				   		           {"cantidad":"2","nombre":"JugoNaranja","precio_unitario":"1800"},\
                                                           {"cantidad":"6","nombre":"Carbon","precio_unitario":"1500"},\
				   		           {"cantidad":"3","nombre":"Pera","precio_unitario":"400"}]]}\
 				                          ']

	##Definiendo el schema para que coincida con el desarrollado en la tarea

	schema = StructType([StructField("compras", ArrayType(ArrayType(StructType([
                            StructField("cantidad",StringType()),
                            StructField("nombre",StringType()),
                            StructField("precio_unitario",StringType())])))),
                            StructField("numero_caja",StringType())])
	##Convirtiendo el json file a dataframe

	_data_js = spark_session.read.schema(schema).json(sc.parallelize(_data_js_string))	

	##El metodo a probar requiere el dataframe despues de haberlo convertido de string a json file	
	##El metodo exploding_data_frame realizara 2 explode para as tener cada producto separado del array

	_dato_calculado = exploding_data_frame(_data_js)	
	
	##Se genera el dataframe esperado con los datos enviados al metodo:

	_dato_esperado = [("3",["2","Harina","1500"]),
			  ("3",["5","Arroz","1000"]),
                          ("3",["4","Frijoles","800"]),
                          ("3",["7","Manzana","500"]),
			  ("3",["2","JugoNaranja","1800"]),
                          ("3",["6","Carbon","1500"]),
			  ("3",["3","Pera","400"])]


	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("col", StructType([
                             StructField("cantidad",StringType()),
                             StructField("nombre",StringType()),
                             StructField("precio_unitario",StringType())]))
                            ])

	##Se obtiene el data frame esperado
	_dato_esperado_df = spark_session.createDataFrame(_dato_esperado,schema)

	_dato_esperado_df.show()
	_dato_calculado.show()
	assert _dato_esperado_df.collect() == _dato_calculado.collect()

### Prueba utilizada para validar que se obtiene correctamente
### la multiplicacion entre cantidad de productos por precio unitario

def test_get_total_precio_data(spark_session):

	_dato = [("3",["2","Harina","1500"]),
	         ("3",["5","Arroz","1000"]),
                 ("2",["4","Frijoles","800"]),
                 ("5",["7","Manzana","500"]),
		 ("4",["2","JugoNaranja","1800"]),
                 ("3",["6","Carbon","1500"]),
		 ("2",["3","Pera","400"])]


	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("col", StructType([
                             StructField("cantidad",StringType()),
                             StructField("nombre",StringType()),
                             StructField("precio_unitario",StringType())]))
                            ])

	_dato_df = spark_session.createDataFrame(_dato,schema)
	_dato_calculado_df = get_total_precio_data(_dato_df)
	
	_dato_esperado =  [("3","2","Harina","1500",3000),
	                   ("3","5","Arroz","1000",5000),
                           ("2","4","Frijoles","800",3200),
                           ("5","7","Manzana","500",3500),
           		   ("4","2","JugoNaranja","1800",3600),
                           ("3","6","Carbon","1500",9000),
       		           ("2","3","Pera","400",1200)]


	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Cantidad", StringType()),
                             StructField("Nombre",StringType()),
                             StructField("Precio Unitario",StringType()),
                             StructField("total_precio_por_producto",IntegerType())])

	_dato_esperado_df = spark_session.createDataFrame(_dato_esperado,schema)
	
	assert _dato_esperado_df.collect() == _dato_calculado_df.collect()

###Prueba utilizada para verificar que se a partir del dataframe que tiene la informacion de todas las cajas y productos
###se puede obtener correctamente la cantidad total que se tiene por cada producto, y se ordena del producto que tiene mas
###cantidad de unidades vendidas hasta el que tiene menos.

def test_get_total_productos_table(spark_session):

	_dato_entrada =  [("3","1","Harina","1500",3000),
	                   ("3","5","Arroz","1000",5000),
                           ("2","4","Frijoles","800",3200),
                           ("5","7","Manzana","500",3500),
           		   ("4","2","JugoNaranja","1800",3600),
                           ("3","6","Carbon","1500",9000),
       		           ("2","3","Pera","400",1200)]


	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Cantidad", StringType()),
                             StructField("Nombre",StringType()),
                             StructField("Precio Unitario",StringType()),
                             StructField("total_precio_por_producto",IntegerType())])
			
	_dato_df = spark_session.createDataFrame(_dato_entrada,schema)
	_dato_calculado_df = get_total_productos_table(_dato_df)

	_dato_esperado = [("Manzana",7),
                           ("Carbon",6),
                           ("Arroz",5),
                           ("Frijoles",4,),
                           ("Pera",3),
                           ("JugoNaranja",2),
                           ("Harina",1)]

	schema = StructType([StructField("Nombre",StringType()),
                             StructField("Total", IntegerType())])

	_dato_esperado_df = spark_session.createDataFrame(_dato_esperado,schema)


	assert _dato_calculado_df.collect() == _dato_esperado_df.collect()

###Prueba utilizada para verificar que a partir del dataframe que tiene toda
###la informacion de cajas y productos, se obtiene la cantidad de total vendida
###por cada caja, y ademas se ordenan de la caja que tiene mayor cantidad de dinero 
###hasta la que tiene menos dinero

def test_get_total_cajas_table(spark_session):

	_dato_entrada =  [("3","1","Harina","1500",3000),
	                   ("3","5","Arroz","1000",5000),
                          ("2","4","Frijoles","800",3200),
                           ("5","7","Manzana","500",3500),
           		   ("4","2","JugoNaranja","1800",3600),
                           ("3","6","Carbon","1500",9000),
       		           ("2","3","Pera","400",1200)]


	schema = StructType([StructField("numero_caja",StringType()),
                            StructField("Cantidad", StringType()),
                             StructField("Nombre",StringType()),
                             StructField("Precio Unitario",StringType()),
                            StructField("total_precio_por_producto",IntegerType())])
			
	_dato_df = spark_session.createDataFrame(_dato_entrada,schema)

	_dato_calculado_df = get_total_cajas_table(_dato_df)

	_dato_esperado = [("3",17000),
		          ("2",4400),
                          ("4",3600),
                          ("5",3500)]

	schema = StructType([StructField("Nombre",StringType()),
                             StructField("Total", IntegerType())])

	_dato_esperado_df = spark_session.createDataFrame(_dato_esperado,schema)


	assert _dato_calculado_df.collect() == _dato_esperado_df.collect()	


### Prueba para verificar que el metodo encargado de obtener la metrica con la caja que mas ventas
### ha hecho, lo hace correctamente

def test_caja_mas_ventas(spark_session):
	_dato_entrada = [("3",20000),
		          ("2",44000),
                          ("4",360000),
                          ("5",350000),
		          ("1",4400),
                          ("6",13600),
                          ("7",7900)]

	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Total Caja", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	str_caja_mas_ventas_calculada = caja_mas_ventas(_dato_entrada_df)
	
	str_caja_mas_ventas_esperada = "4"
	assert str_caja_mas_ventas_calculada == str_caja_mas_ventas_esperada


##Metodo encarga de verificar que el calculo para determinar la caja que tiene menos ventas se hace
##correctamente.

def test_caja_menos_ventas(spark_session):
	_dato_entrada =  [("3",20000),
		          ("2",44000),
                          ("4",360000),
                          ("5",350000),
		          ("1",4400),
                          ("6",13600),
                          ("7",7900)] 

	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Total Caja", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	str_caja_menos_ventas_calculada = caja_menos_ventas(_dato_entrada_df)
	
	str_caja_menos_ventas_esperada = "1"

	assert str_caja_menos_ventas_calculada == str_caja_menos_ventas_esperada

### Metodo encargado de verificar que el producto mas vendido es seleccionado de manera correcta

def test_producto_mas_vendido(spark_session):

	_dato_entrada = [("Manzana",21),
                           ("Carbon",69),
                           ("Arroz",52),
                           ("Frijoles",41,),
                           ("Pera",31),
                           ("JugoNaranja",22),
                           ("Harina",14)]

	schema = StructType([StructField("Nombre",StringType()),
                             StructField("Total", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	str_producto_mas_vendido_calculado = producto_mas_vendido(_dato_entrada_df)
	
	str_producto_mas_vendido_esperado = "Carbon"

	assert str_producto_mas_vendido_calculado == str_producto_mas_vendido_esperado

### Metodo encarga de verificar que se selecciona adecuadamente el producto que tiene un mayor ingreso

def test_producto_mayor_ingreso(spark_session):

	_dato = [("3","2","Harina","1500",3000),
	         ("3","5","Arroz","1000",5000),
                 ("2","4","Frijoles","800",3200),
                 ("5","7","Manzana","500",3500),
           	 ("4","8","JugoNaranja","1800",14400),
                 ("3","6","Carbon","1500",9000),
       		 ("2","3","Pera","400",1200)]


	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Cantidad", StringType()),
                             StructField("Nombre",StringType()),
                             StructField("Precio Unitario",StringType()),
                             StructField("total_precio_por_producto",IntegerType())])


	_dato_df = spark_session.createDataFrame(_dato,schema)

	_dato_calculado_str = producto_mayor_ingreso(_dato_df)	
	
	_dato_esperado_str = "JugoNaranja"
	
	assert _dato_calculado_str==_dato_esperado_str

### Metodo encargado de verificar que se obtiene el percentil 25 correctamente de la caja que ha vendido de menor a mayor
def test_check_percentil_25_caja(spark_session):
	_dato_entrada =  [("3",20000),
		          ("2",44000),
                          ("4",360000),
                          ("5",350000),
		          ("1",4400),
                          ("6",13600),
                          ("7",7900)]  

	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Total Caja", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	_percentile_calculado = percentiles_caja(_dato_entrada_df)
	_percentile_esperado = ("7")

	assert _percentile_calculado[0]==_percentile_esperado

### Metodo encargado de verificar que se obtiene el percentil 50 correctamente de la caja que ha vendido de menor a mayor

def test_check_percentil_50_caja(spark_session):
	_dato_entrada =  [("3",20000),
		          ("2",44000),
                          ("4",360000),
                          ("5",350000),
		          ("1",4400),
                          ("6",13600),
                          ("7",7900)]  

	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Total Caja", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	_percentile_calculado = percentiles_caja(_dato_entrada_df)
	_percentile_esperado = ("3")

	assert _percentile_calculado[1]==_percentile_esperado

### Metodo encargado de verificar que se obtiene el percentil 75 correctamente de la caja que ha vendido de menor a mayor

def test_check_percentil_75_caja(spark_session):
	_dato_entrada =  [("3",20000),
		          ("2",44000),
                          ("4",360000),
                          ("5",350000),
		          ("1",4400),
                          ("6",13600),
                          ("7",7900)]  

	schema = StructType([StructField("numero_caja",StringType()),
                             StructField("Total Caja", IntegerType())])

	_dato_entrada_df = spark_session.createDataFrame(_dato_entrada,schema)

	_percentile_calculado = percentiles_caja(_dato_entrada_df)
	_percentile_esperado = ("5")

	assert _percentile_calculado[2]==_percentile_esperado

