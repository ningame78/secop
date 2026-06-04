%md
# 🏛️ Auditoría Analítica SECOP II - Pipeline de Ingesta y Modelo NoSQL
### Proyecto Final de Big Data & Data Lakehouse

---

## 👥 1. Integrantes del Equipo
* **Miguel**
* **Zuly Nathaly**
* **Nino Valoyes**

---

## 📅 2. Control de Trazabilidad e Integridad (Data Lineage)
* **Fecha y Hora de Cierre de Descarga:** 3 de junio de 2026 - 09:00 PM
* **Universo de Registros Procesados:** `1,687,554` filas consolidadas.

---

## 📐 3. Decisiones de Arquitectura
El ecosistema se diseñó bajo una arquitectura **Data Lakehouse** distribuida utilizando **PySpark (Spark SQL)** sobre nodos **Databricks Serverless**, garantizando un procesamiento eficiente en memoria para los más de 1.6 millones de transacciones. 

El pipeline está estrictamente dividido en dos componentes lógicos independientes (Notebooks):

📂 taller_final/
├── 📓 Taller_final_descarga.ipynb  (Capa Bronze / Ingesta Resiliente)

     └── 📓 Taller_final_analisis.ipynb  (Capa Silver y Gold / Modelo NoSQL)

### Componente 1: Taller_final_descarga (`Notebook 1`)
1. **Auditoría Previa:** Realiza consultas directas a los endpoints mediante `count(*)` para verificar el universo total de registros disponibles en la API de Socrata de los años objetivo (**2021** y **2025**).
2. **Estrategia de Chunking:** Descarga de datos de forma paginada mediante bloques masivos de **50,000 registros** a través de parámetros límites (`$limit`) y desplazamientos (`$offset`).
3. **Mecanismo de Checkpoints (Puntos de Control):** Antes de iniciar cada iteración, el script escanea el almacenamiento físico. Si detecta una caída de red o desconexión del clúster, el pipeline **reanuda automáticamente** en el último bloque guardado con éxito, protegiendo la consistencia de los archivos binarios **Parquet** de la capa Bronze.

### Componente 2: Taller_final_analisis (`Notebook 2`)
Encargado de la transformación, limpieza avanzada de datos huérfanos y la estructuración del modelo documental NoSQL.

---

## 🏗️ 4. Modelo Documental NoSQL (Colecciones Exportadas)
Tras enriquecer y cruzar los datos de contratos con la tabla geoespacial **DIVIPOLA (DANE)**, la data se transformó en estructuras semiestructuradas (BSON/JSON) optimizadas para **MongoDB Atlas** para alimentar la capa del Dashboard analítico.

Antes de su generación, el pipeline realiza un proceso automático de **Force Purge** (vaciado absoluto del directorio raíz) para garantizar la frescura de los datos. Las 6 colecciones estructuradas son:

1. 📦 **`contratos_operativos`**: Contratos con metadatos y subdocumentos anidados de `detalles_financieros` y `ubicacion_geografica` mediante funciones `F.struct` para evitar JOINs en caliente.
2. 🚨 **`alertas_revision`**: Colección exclusiva de contratos catalogados con prioridad **ALTA** que contiene el desglose analítico de su matriz de puntos de riesgo.
3. 🏛️ **`entidades_resumen`**: Colección pre-agregada por Departamento que almacena presupuestos totales y scores de riesgo promedio (*Pattern Computed*).
4. 🤝 **`proveedores_resumen`**: Agregación analítica agrupada por municipio para evaluar niveles de liquidez recibida.
5. 🏷️ **`temas_resumen`**: Clúster de contratos consolidados por temáticas críticas de interés fiscal (ej. PAE, Agua Potable).
6. ⚙️ **`metadata_pipeline`**: Documento de auditoría técnica que certifica el éxito del proceso (`SUCCESS`), fecha de ejecución (`2026-06-03`) y número total de filas inyectadas.

---

## ⚙️ 5. Variables de Entorno Requeridas
Para la correcta ejecución del Notebook de Análisis y la posterior migración hacia MongoDB Atlas, es obligatorio configurar los siguientes parámetros secretos en el entorno de Databricks:

* `MONGO_URI`: Cadena de conexión estricta de producción (`mongodb+srv://...`) que apunta al clúster en la nube de MongoDB Atlas.
* `MONGO_DB`: Nombre de la base de datos destino (`taller_final`).
* `SPARK_LOCAL_DIR`: Directorio de intercambio temporal para el procesamiento de los JSON intermedios en Spark Serverless.

---

## ⚠️ 6. Limitaciones Encontradas y Estrategias de Solución
* **Saturación Crítica de la API de Socrata (Error 429/Timeouts):** Las consultas de conteo y descarga masiva de años cerrados bloqueaban constantemente las llamadas por exceder los tiempos de espera del servidor público. **Solución:** Se subió el `timeout` de red a 90 segundos y se inyectó un algoritmo de reintentos robustos con pausas preventivas de 30 segundos.
* **Corrupción de Esquemas en Modificaciones:** La tabla de adiciones (`cb9c-h8sn`) no contenía montos financieros ejecutables en producción. **Solución:** El cálculo del valor adicionado se dedujo matemáticamente mediante la diferencia de los campos nativos de la tabla maestra de contratos.
* **Inconsistencia de Datos Geográficos (Huérfanos al 18.73%):** Los funcionarios registran nombres de municipios con graves errores ortográficos, nulos absolutos o texto basura (ej: *Bogota D.C.*, *Cartagena D.T.*, *No Definido*), impidiendo el cruce exacto con la DIVIPOLA. **Solución:** Se diseñó un **Algoritmo de Extracción Inversa** con expresiones regulares agresivas y cruce por contención de texto bidireccional, **desplomando la tasa de error al 6%**, justificando el margen restante mediante un reporte impreso de campos nulos desde la API de origen.

---

## 🚀 7. Instrucciones para Reproducir el Pipeline

> ⚠️ **Requisito Previo:** Verificar que la tabla base `divipola.parquet` se encuentre depositada en la ruta `/Volumes/workspace/default/taller_final/raw/divipola/`.

1. **Paso 1:** Abrir Databricks y ejecutar por completo el notebook `Taller_final_descarga`. Esto poblará el almacenamiento con las particiones limpias de contratos, adiciones y ejecuciones de los años 2021 y 2025.
2. **Paso 2:** Importar y abrir el notebook `Taller_final_analisis`.
3. **Paso 3:** Ejecutar la celda de inicialización para realizar la purga física preventiva de la caché analítica anterior.
4. **Paso 4:** Correr secuencialmente las secciones de limpieza, agregación y el modelo de priorización en cascada (Actividad 4).
5. **Paso 5:** Ejecutar la Actividad 5 para materializar las 6 colecciones JSON estructuradas en el directorio `/Volumes/workspace/default/taller_final/raw/mongodb_collections/`.
6. **Paso 6:** Utilizar la herramienta `mongoimport` o el conector nativo para migrar los archivos JSON resultantes a MongoDB Atlas y conectar el Dashboard.