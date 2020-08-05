from .programaestudiante import genera_data_frame_estudiantes
from .programaestudiante import genera_data_frame_notas
from .programaestudiante import genera_data_frame_cursos
from .programaestudiante import join_cursos_notas
from .programaestudiante import join_notas_estudiantes
from .programaestudiante import calcula_total_creditos_estudiante
from .programaestudiante import calcula_promedio_estudiantes
from .programaestudiante import calcula_n_mejores_promedios_carrera

####
## Las siguientes tres pruebas se encargar de cargar los archivos csv 
## para verificar que los mismos no estan vacios

def test_genera_data_frame_estudiantes(spark_session):
	estudiantes_programa = genera_data_frame_estudiantes("estudiante.csv")
	##Validando que estudiante.csv no esta vacio
	assert estudiantes_programa.count() > 0
	

def test_genera_data_frame_notas(spark_session):
	notas_programa = genera_data_frame_estudiantes("nota.csv")
	##Validando que notas.csv no esta vacio
	assert notas_programa.count() > 0

def test_genera_data_frame_cursos(spark_session):
	curso_programa = genera_data_frame_estudiantes("curso.csv")
	##Validando que curso.csv no esta vacio
	assert curso_programa.count() > 0
	

### La siguiente prueba se encargar de verificar el funcionamiento del metodo que hace
### el join del csv que contiene los datos de los cursos
### y el csv que tiene los datos de las notas de los estudiantes
### 

def test_join_cursos_notas(spark_session):

	cursos_data = [(2000,4,"Carrera1"),
                       (2001,3,"Carrera2"),
                       (2002,2,"Carrera3"),
                       (2003,1,"Carrera4")]
	cursos_df = spark_session.createDataFrame(cursos_data,
	                    ['codigo_curso', 'creditos','carrera'])

	nota_data = [(200000000,2000,95),
                     (200000001,2001,90),
                     (200000002,2002,80),
                     (200000003,2003,70)]
	nota_df = spark_session.createDataFrame(nota_data,
	                    ['carnet', 'codigo_curso','Nota'])
	resultado_obtenido = join_cursos_notas(cursos_df,nota_df)
	
	resultado_esperado =  spark_session.createDataFrame(
         		[(200000003, 2003, 1, 70,"Carrera4"),
	                 (200000002, 2002, 2, 80,"Carrera3"),
	                 (200000000, 2000, 4, 95,"Carrera1"),
		         (200000001, 2001, 3, 90,"Carrera2")],
                        ['carnet', 'codigo_curso', 'creditos', 'Nota','carrera'])
	assert resultado_obtenido.collect() == resultado_esperado.collect()	

### La siguiente prueba se encarga de verificar el funcionamiento del
### metodo que hace el join de las tablas de cursos y notas con la tabla
### que contiene la informacion relacionada a los estudiantes
### Esta seria la tabla que ya tendria la informacion de las tres tablas
### en una sola

def test_join_notas_estudiantes(spark_session):
	estudiantes_data = [(201204347,"Pablo Bolanos","Electronica"),
                            (201124660,"Rocio Salas","Electronica")]
	estudiantes_df = spark_session.createDataFrame(estudiantes_data,
	                    ['carnet', 'nombre','carrera'])

	data_to_join = [(201204347,4514,2,98,"Electronica"),
	                (201204347,4528,2,95,"Electronica"),
                        (201204347,4540,2,99,"Electronica"),
                        (201204347,4519,2,98,"Electronica"),
                        (201204347,4530,3,73,"Electronica"),
                        (201204347,4527,3,78,"Electronica"),
                        (201204347,4522,1,74,"Electronica"),
			(201124660,4536,4,75,"Electronica"),
                        (201124660,4549,4,72,"Electronica"),
                        (201124660,4513,2,89,"Electronica"),
                        (201124660,4541,2,95,"Electronica"),
                        (201124660,4546,4,62,"Electronica"),
                        (201124660,4523,4,97,"Electronica"),
                        (201124660,4546,4,99,"Electronica")]

	data_to_join_df = spark_session.createDataFrame(data_to_join,
                                    ['carnet','codigo_curso','creditos','Nota','carrera'])

	result_from_join_obtenido = join_notas_estudiantes(data_to_join_df,estudiantes_df)

	result_from_join_esperado = [(201204347,"Pablo Bolanos",4514,2,98,"Electronica"),
		            (201204347,"Pablo Bolanos",4528,2,95,"Electronica"),
    			    (201204347,"Pablo Bolanos",4540,2,99,"Electronica"),
      		            (201204347,"Pablo Bolanos",4519,2,98,"Electronica"),
                            (201204347,"Pablo Bolanos",4530,3,73,"Electronica"),
                            (201204347,"Pablo Bolanos",4527,3,78,"Electronica"),
                            (201204347,"Pablo Bolanos",4522,1,74,"Electronica"),
                            (201124660,"Rocio Salas",4536,4,75,"Electronica"),
                            (201124660,"Rocio Salas",4549,4,72,"Electronica"),
                            (201124660,"Rocio Salas",4513,2,89,"Electronica"),
                            (201124660,"Rocio Salas",4541,2,95,"Electronica"),
                            (201124660,"Rocio Salas",4546,4,62,"Electronica"),
                            (201124660,"Rocio Salas",4523,4,97,"Electronica"),
                            (201124660,"Rocio Salas",4546,4,99,"Electronica")]

	result_from_join_esperado_df = spark_session.createDataFrame(result_from_join_esperado,
                           ['carnet','nombre','codigo_curso','creditos','Nota','carrera'])

	assert result_from_join_obtenido.collect()  == result_from_join_esperado_df.collect()


### Esta prueba se encarga de verificar que el metodo que hace las sumas 
### de los creditos de cada estudiante se realiza correctamente
### El metodo debe de tomar todos los cursos de cada estudiante, sumar los creditos
### y dar como resultado una tabla donde aparece el total de creditos y la suma de 
### de las notas de todos los cursos.

def test_calcula_total_creditos_estudiante(spark_session):
	notas_estudiantes = [(201204347,"Pablo Bolanos",4514,2,98,"Electronica"),
		            (201204347,"Pablo Bolanos",4528,2,95,"Electronica"),
    			    (201204347,"Pablo Bolanos",4540,2,99,"Electronica"),
      		            (201204347,"Pablo Bolanos",4519,2,98,"Electronica"),
                            (201204347,"Pablo Bolanos",4530,3,73,"Electronica"),
                            (201204347,"Pablo Bolanos",4527,3,78,"Electronica"),
                            (201204347,"Pablo Bolanos",4522,1,74,"Electronica"),
                            (201124660,"Rocio Salas",4536,4,75,"Computacion"),
                            (201124660,"Rocio Salas",4549,4,72,"Computacion"),
                            (201124660,"Rocio Salas",4513,2,89,"Computacion"),
                            (201124660,"Rocio Salas",4541,2,95,"Computacion"),
                            (201124660,"Rocio Salas",4546,4,62,"Computacion"),
                            (201124660,"Rocio Salas",4523,4,97,"Computacion"),
                            (201124660,"Rocio Salas",4546,4,99,"Computacion"),
                            (200906573,"Xinia Dobles",4823,4,64,"Industrial"),
                            (200906573,"Xinia Dobles",4818,3,86,"Industrial"),
                            (200906573,"Xinia Dobles",4805,2,95,"Industrial"),
                            (200906573,"Xinia Dobles",4824,3,86,"Industrial"),
                            (200906573,"Xinia Dobles",4842,3,71,"Industrial"),
                            (200906573,"Xinia Dobles",4828,1,88,"Industrial"),
                            (200906573,"Xinia Dobles",4823,4,69,"Industrial")]

	notas_estudiantes_df = spark_session.createDataFrame(notas_estudiantes,
                           ['carnet','nombre','codigo_curso','creditos','Nota','carrera'])

	resultado_calculado_df = calcula_total_creditos_estudiante(notas_estudiantes_df)

	resultado_esperado = [(201124660,"Rocio Salas","Computacion",24,1988),
                              (200906573,"Xinia Dobles","Industrial",20,1539),
                              (201204347,"Pablo Bolanos","Electronica",15,1307)]

	resultado_esperado_df = spark_session.createDataFrame(resultado_esperado,
                            ['carnet','nombre','carrera','creditos_cursados','notas_sumadas'])


	assert resultado_calculado_df.collect() == resultado_esperado_df.collect()

### Esta prueba se encarga de verificar que el metodo que se encarga de obtener el promedio
### de cada estudiante, lo realice de manera correcta
### Al final, se generara una tabla que tiene los estudiantes ordenados del mejor promedio al peor
### promedio de todas las carreras.

def test_calcula_promedio_estudiantes(spark_session):
	creditos_totales_estudiantes = [(201298136,"Lissette Maroto","Administracion",20,1960),
                                       (201427184,"Amelia Romero","Computacion",16,1520),
                                       (200939705,"Melany Serrano","Industrial",19,1710),
                                       (201031261,"Pamela Navas","Industrial",18,1584)]

	creditos_totales_estudiantes_df = spark_session.createDataFrame(creditos_totales_estudiantes,
                                                             ["carnet","nombre","carrera","creditos_cursados","notas_sumadas"])


	resultado_calculado_df = calcula_promedio_estudiantes(creditos_totales_estudiantes_df)

	resultado_esperado = [(201298136,"Lissette Maroto","Administracion",20,1960,98.0),
                              (201427184,"Amelia Romero","Computacion",16,1520,95.0),
                              (200939705,"Melany Serrano","Industrial",19,1710,90.0),
                              (201031261,"Pamela Navas","Industrial",18,1584,88.0)]

	resultado_esperado_df = spark_session.createDataFrame(resultado_esperado,
                                                        ["carnet","nombre","carrera","creditos_cursados","notas_sumadas","Promedio"])

	assert resultado_calculado_df.collect() == resultado_esperado_df.collect()

### Finalmente, esta prueba verifica que el metodo encargado de obtener los n mejores promedios de cada 
### carrera, lo haga adecuadamente. El metodo recibe como parametor n, que por el momento se tiene en 2

def test_calcula_n_mejores_promedios_carrera(spark_session):

	promedio_estudiantes = [(200963493,"XiniaLopez","Mantenimiento",16,1472,92.0),
                                (201788127,"GerardoGuardia","Mantenimiento",12,1092,91.0),
                                (201086048,"MarcoCordero","Mantenimiento",20,1800,90.0),
                                (201567750,"MarcoArrieta","Mantenimiento",18,1584,88.0),
                                (201347020,"LissetteFernandez","Mantenimiento",17,1462,86.0),
                                (201169188,"XiniaOrtiz","Mantenimiento",15,1275,85.0),
                                (201680416,"HeizelBenavidez","Industrial",19,1805,95.0),
                                (200926538,"LuisMurillo","Industrial",18,1656,92.0),
                                (201688123,"MarcoChinchilla","Industrial",20,1780,89.0),
                                (201179436,"HeizelCordero","Industrial",15,1320,88.0),
                                (201304095,"JorgeRojas","Industrial",16,1376,86.0),
                                (201541413,"LourdesNavas","Industrial",16,1360,85.0)]

	promedio_estudiantes_df = spark_session.createDataFrame(promedio_estudiantes,
                                                        ["carnet","nombre","carrera","creditos_cursados","notas_sumadas","Promedio"])

	resultado_obtenido_df = calcula_n_mejores_promedios_carrera(promedio_estudiantes_df,2)

	resultado_esperado = [("Mantenimiento",200963493,"XiniaLopez",92.0,1),
                              ("Mantenimiento",201788127,"GerardoGuardia",91.0,2),
                              ("Industrial",201680416,"HeizelBenavidez",95.0,1),
                              ("Industrial",200926538,"LuisMurillo",92.0,2)]

	resultado_esperado_df = spark_session.createDataFrame(resultado_esperado,
                                                    ["carrera","carnet","nombre","Nota","rank"])

	assert resultado_esperado_df.collect() == resultado_obtenido_df.collect()


