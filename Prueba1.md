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
Origen:
Conjunto de datos origen=SourceDataset_xer
Propiedades= Nombre= vTabla , Valor= @item().tabla , Tipo=String
Utilizar= Tabla
![image](https://user-images.githubusercontent.com/108036215/176327756-1e1aa783-7d5e-48c3-9868-afdf53a9b0cf.png)



               


