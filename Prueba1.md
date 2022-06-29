# Examen
## Descripción
 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

## Desarrollo
### Intregación y Canalización
Se crea el fgarzon_Pipeline 
## Lookup 
   Para recuperar el conjunto de datos
   - Nombre= Tablas
### Configuración
   Conjunto de datos origen=SourceDataset_xer
 
   Propiedades= Nombre= vTabla , Valor= Cliente , Tipo=String
               
   Consulta:
```
select 
table_name as tabla
from information_schema. tables
where
table_schema='dwh'
```

![image](https://user-images.githubusercontent.com/108036215/176326938-adeccb10-3b64-494d-96c1-2773087e287d.png)

## ForEach
Nombre= ForEach1
### Configuración
Elemento= @activity('Tablas').output.value
### Actividad
Nombre= CopiarTabla

**Origen:**

Conjunto de datos origen=SourceDataset_xer

Propiedades:  Nombre= vTabla , Valor= @item().tabla , Tipo=String

Utilizar= Tabla

![image](https://user-images.githubusercontent.com/108036215/176327756-1e1aa783-7d5e-48c3-9868-afdf53a9b0cf.png)

**Receptor:**

Conjunto de datos receptor=DestinationDataset_xer

Propiedades:  Nombre= vTabla , Valor= @concat(item().Tabla,'_fgarzon') , Tipo=String

![image](https://user-images.githubusercontent.com/108036215/176328343-ef2ec3a4-9ada-4ed4-bbd0-afcea14bb14e.png)

### Depurar:
Una vez configurado  enviamos a depurar

![image](https://user-images.githubusercontent.com/108036215/176328453-4854cabf-0643-41a0-8b2b-62b4870607fd.png)

##Datos - Vinculados

Se valida la creacion de los archivos parquet de las tablas:

- cliente_fgarzon.parquet
- factura_fgarzon.parquet
- facturaproducto_fgarzon.parquet
- producto_fgarzon.parquet

![image](https://user-images.githubusercontent.com/108036215/176328786-ccf9230c-c14f-4502-8e0a-297b0d2a11e4.png)


##Notebook
Se crea el Notebook_fgarzon




               


