# Examen
## Descripción
Dado el siguiente modelo analítico.
 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

Las tablas del modelo analítico proveen los datos transaccionales. Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.

Lineamientos generales:

- El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
- Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline

#Carga de Archivo:
- Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
```
 %%pyspark
df = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/fgarzon/clientes_correos.csv', format='csv')
display(df.limit(10))
```


- Los notebooks de Spark deben ser nombrados  {usuario}_notebook
- La tabla de resultados debe almacenarse en un POOL SQL, en el esquema default con el esquema "tbl_{usuario}"

Finalmente, cada participante deberá elaborar un archivo Markdown en Github con acceso publico. Por su puesto en el documento debe obviarse datos sensibles como claves etc.
El documento Markdown deberá contener el proceso técnico que el participante ha seguido con un orden lógico y el link del proyecto Githab deberá ser registrado como entregable de esta tarea.
