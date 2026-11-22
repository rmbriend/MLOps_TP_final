# Clasificación estelar: instalación y ejecución desde un clon nuevo

Esta guía corresponde al flujo **SDSS → MinIO → notebook/Optuna → MLflow → web**
de `MLOps_TP_final`. Los comandos están escritos para **Windows y PowerShell** y
se ejecutan desde la raíz del repositorio, en la carpeta donde se haya clonado o
extraído. No requieren la ruta del autor, Anaconda ni el repositorio original.

El clon contiene código y configuración. Cada usuario genera sus datos,
experimentos y modelo `champion`; no recibe los volúmenes Docker de otro equipo.
Compose levanta la infraestructura. El entrenamiento puede ejecutarse con el DAG
`train_star_model` o con la notebook; ambos usan el mismo código. La web se inicia
en Windows después de disponer de un champion, siguiendo el paso 4.

## 1. Requisitos y arranque

- Docker Desktop iniciado, usando contenedores Linux y Docker Compose moderno
  (`docker compose`, versión 2.14 o posterior).
- Python de 64 bits, versión 3.11, 3.12 o 3.13; se recomienda 3.12.
- VS Code con las extensiones Python/Jupyter, o JupyterLab para abrir la notebook.
- Acceso a Docker Hub, PyPI y `skyserver.sdss.org`.
- Al menos 4 GB disponibles para Docker; es preferible asignar 8 GB si el equipo
  lo permite. Airflow, el build y el entrenamiento pueden tardar varios minutos.
  Véanse los [requisitos de Airflow 3.0.2](https://airflow.apache.org/docs/apache-airflow/3.0.2/howto/docker-compose/index.html).

### 1.1. Iniciar Docker Desktop

Abrir **Docker Desktop** desde el menú Inicio de Windows y esperar que el motor
esté en ejecución. Este proyecto usa contenedores **Linux**.

En PowerShell, comprobar:

```powershell
docker version
docker compose version
```

`docker version` debe mostrar **Client y Server**. Si solo aparece Client, no
continuar con Compose: esperar que Docker Desktop termine de iniciar. Si aparece
un error de `dockerDesktopLinuxEngine`, revisar Docker Desktop y volver a ejecutar
`docker version` hasta que también responda Server.


### 1.4. Construir y levantar los contenedores

Primero validar la configuración. Si es válida, `config --quiet` termina sin
imprimir errores. Después iniciar los servicios:

```powershell
docker compose --profile airflow --profile mlflow config --quiet
docker compose --profile airflow --profile mlflow up -d --build
```

`--build` construye las imágenes y `-d` deja los contenedores ejecutándose en
segundo plano. La primera construcción descarga imágenes y dependencias y puede
tardar varios minutos. Esperar que termine sin errores antes de pasar al DAG.

El comando inicia PostgreSQL, Redis, MinIO, MLflow y los componentes de Airflow.
También ejecuta la inicialización de Airflow y la creación de los buckets mediante
las dependencias configuradas en Compose; no hace falta inicializarlos a mano.
La notebook y la web estelar se ejecutan después, por separado.

### 1.5. Confirmar que todos estén listos

```powershell
docker compose --profile airflow --profile mlflow ps -a
```

Esperar que los servicios persistentes estén `healthy`: `postgres`, `redis`,
`s3` (contenedor `minio`), `mlflow`, `airflow-apiserver`, `airflow-scheduler`,
`airflow-dag-processor`, `airflow-triggerer` y **`airflow-worker`**.
Repetir `ps -a` después de unos minutos si todavía están arrancando.

`health: starting` durante el arranque no significa que hayan fallado.
`airflow-init` y `create_s3_buckets` son tareas de inicialización: es normal que
terminen como `Exited (0)`. Además, confirmar en la consola MinIO que existen
los buckets **`data`** y **`mlflow`**; el código de salida del inicializador por sí
solo no confirma su creación.

Si falta un servicio, aparece `Exited` con un código distinto de cero o se mantiene
en `unhealthy`, revisar los logs antes de ejecutar el DAG:

```powershell
docker compose --profile airflow --profile mlflow logs --tail 100 airflow-init airflow-worker airflow-scheduler airflow-apiserver mlflow s3 create_s3_buckets
```

Para volver a levantar el proyecto después de apagar el equipo, iniciar primero
Docker Desktop y repetir el comando `up -d --build` desde esta misma carpeta.
Los volúmenes existentes conservan datos, experimentos y modelos; no hace falta
entrenar de nuevo si el champion ya está guardado. Para detenerlos conservando datos, usar
`docker compose --profile airflow --profile mlflow down`, sin agregar `-v`.

### 1.6. Acceder a los servicios

Una vez saludables, abrir las siguientes direcciones en el navegador:


| Servicio | Dirección desde Windows | Acceso predeterminado |
|---|---|---|
| Airflow | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| MLflow | [http://localhost:5001](http://localhost:5001) | Sin login en esta configuración local |
| MinIO, consola | [http://localhost:9001](http://localhost:9001) | `minio` / `minio123` |
| MinIO, API S3 | `http://localhost:9000` | Las mismas credenciales MinIO |
| Web estelar, después del paso 4 | [http://localhost:8801](http://localhost:8801) | Sin login local |

Los valores son para desarrollo local. Si se cambian credenciales o puertos,
actualizar también los clientes. Las credenciales de Airflow se crean en la
inicialización; editar `.env` no cambia automáticamente usuarios ya existentes.

Dentro de Docker se usan `http://mlflow:5000` y `http://s3:9000`; desde Windows se
usan los puertos publicados de la tabla. `localhost` dentro de un contenedor
designa ese mismo contenedor.

Si otro clon usa los contenedores `postgres`, `minio`, `mlflow` o
`airflow-apiserver`, detenerlo desde su propia carpeta antes de arrancar este.
Cambiar solo el nombre de la carpeta no evita los conflictos de nombres y puertos.
El perfil `all` incluye además la API del ejemplo Heart Disease en 8800, que no
interviene en el flujo estelar. Usar los perfiles indicados arriba.

## 2. Obtener los datos con el DAG

En Airflow, iniciar sesión, buscar **`process_etl_star_data`**, habilitarlo y usar
**Trigger DAG**. Esperar el estado **Success** de su tarea `fetch_and_store_starset`.
El DAG es manual: no se ejecuta automáticamente al levantar Compose.

El archivo `airflow/dags/etl_star_data.py` aparece en Airflow porque Compose monta
la carpeta `airflow/dags`. No es necesario copiarlo desde la interfaz. Si no aparece:

```powershell
docker compose --profile airflow --profile mlflow exec airflow-scheduler airflow dags list-import-errors
docker compose --profile airflow --profile mlflow logs --tail 100 airflow-dag-processor
```

El DAG consulta SDSS DR17 mediante la API SQL, une `PhotoObj` y `SpecObj`, filtra
`scienceprimary = 1` y toma hasta 1000 filas aleatorias con `ORDER BY NEWID()`.
Cada nueva ejecución de ingesta solicita otra muestra, sin balancear las clases. Guarda:

- CSV original validado: `s3://data/raw/star/starset.csv`.
- Copia identificada por hash: `s3://data/raw/star/snapshots/<sha256>.csv`.
- Experimento **`star type`** en MLflow y un run de ingesta con el hash y la consulta.

Confirmar esos resultados en MinIO y MLflow antes de entrenar. El ID numérico del
experimento puede variar entre equipos: el código lo busca por su nombre.
Este DAG realiza la ingesta; `train_star_model` o la notebook realizan el entrenamiento.
Para entrenar con datos nuevos, ejecutar primero otra ingesta y luego el entrenamiento.
Reentrenar sin una ingesta nueva reutiliza el snapshot existente. El experimento
sigue llamándose `star type`; cada ingesta y búsqueda se identifican por sus runs.
MLflow guarda `sampling_method=random_order_by_newid`, la consulta SQL y el hash.
La muestra aleatoria no tiene semilla SQL; el CSV conservado permite reproducirla.
Los snapshots antiguos, obtenidos por objid, siguen siendo válidos y no se modifican.

## 3. Entrenar, evaluar y promover: DAG o notebook

El paquete `airflow/dags/star_training/` contiene la lógica compartida de
preparación D2, búsqueda Optuna, entrenamiento, evaluación, registro y promoción.
Elegir una de las dos alternativas siguientes: no hace falta ejecutar ambas.

### 3.1. Ejecutar el DAG de entrenamiento

Después de actualizar el repositorio, reconstruir las dependencias de Airflow y
recrear los servicios que lo necesiten desde la raíz del repo:

```powershell
docker compose --profile airflow --profile mlflow up -d --build
docker compose --profile airflow --profile mlflow ps -a
```

Esperar a que Airflow esté saludable. La nueva imagen incluye Optuna y
scikit-learn 1.7.2; no alcanza con reiniciar un worker que todavía usa la imagen
anterior. Las seis tareas usan `ExternalPythonOperator` con el entorno aislado
`/home/airflow/star-venv`, preinstalado en la imagen; sus versiones de numpy/pandas
no modifican las de los proveedores internos de Airflow. No se descargan paquetes
dentro de cada tarea.

1. Completar primero `process_etl_star_data` para generar el snapshot de ingesta.
2. En Airflow, habilitar **`train_star_model`** y pulsar **Trigger DAG**.
3. Revisar los parámetros del formulario y lanzar la ejecución manual.

| Parámetro | Predeterminado | Uso |
|---|---|---|
| `n_trials` | 250 | Búsqueda completa; usar 2 o 5 para una prueba breve. |
| `seed` | 42 | Partición train/test y semilla de Optuna. |
| `snapshot_sha256` | Sin valor (`null`) | Última ingesta exitosa al iniciar; opcionalmente indicar un hash concreto. |
| `min_delta` | 0.0 | Se exige F1 candidato > F1 champion + este valor; 0.01 equivale a un punto de F1. |

El DAG no se dispara automáticamente con la ingesta. Su ejecución tiene seis
tareas visibles de forma independiente:

| Tarea | Resultado |
|---|---|
| `resolve_snapshot` | Fija el CSV por SHA-256 y comprueba su integridad. |
| `search_hyperparameters` | Optuna CV5, selección y runs hijos en MLflow. |
| `train_and_log_model` | Pipeline completo con scaler y artefacto del candidato. |
| `evaluate_candidate` | Recarga del modelo, F1 weighted/macro, reporte y gráficos de test. |
| `register_candidate` | Versión del modelo `star_type_model_prod`, sin mover el alias. |
| `compare_and_promote_champion` | Compara ambas versiones y decide si cambia `champion`. |

**La promoción es una tarea separada del entrenamiento y del registro.** Allí se
evalúa al candidato y al champion vigente sobre exactamente las mismas filas y
columnas de test. No se comparan métricas históricas de datasets diferentes.

- Mejora estricta del F1 weighted: promueve al candidato.
- Empate o resultado inferior: conserva el champion; la tarea termina en Success.
- Sin champion: promueve el primer candidato que pase la carga y validación.
- Error al cargar/evaluar, esquema incorrecto o alias cambiado durante la
  comparación: falla la tarea antes de actualizar el alias.

En los logs y el XCom de `compare_and_promote_champion` se ven `promoted`,
`reason`, versiones, ambos F1, SHA del dataset y cantidad de filas evaluadas.
En el run padre de MLflow se guarda **`promotion/comparison.json`**, con la
decisión y sus métricas. Las versiones anteriores permanecen en el registro.
La web detecta un nuevo champion al refrescar su caché, hasta un minuto después.

Datos y checkpoints permanecen en S3; XCom solo transmite referencias pequeñas.
Los checkpoints están en `s3://data/training/star/<hash_de_ejecucion>/`.
Si falla evaluación, registro o comparación, corregir el problema y limpiar/reintentar
solo esa tarea y las posteriores de la misma ejecución. No repetir la búsqueda.
Las etapas ya completadas recuperan sus checkpoints, y un registro ya creado se
reutiliza. Una búsqueda interrumpida antes de completarse puede repetir sus trials.
Para una búsqueda nueva o una nueva decisión sobre otro champion, crear otro Dag Run.

Solo se permite un Dag Run de entrenamiento activo. No ejecutar simultáneamente
otro promotor desde la notebook o desde la interfaz: MLflow no ofrece una escritura
condicional atómica del alias entre aplicaciones.

La muestra aleatoria proviene del catálogo espectroscópico y no garantiza balance
de clases ni ausencia de sesgos del catálogo. El F1 macro y el reporte por clase se registran,
pero la regla de promoción usa únicamente F1 weighted. El test participa en la
decisión de promoción; no es un benchmark independiente de todas las promociones.
Si un champion antiguo vio objetos de un snapshot nuevo, puede existir solapamiento:
para una evaluación científica, reservar un conjunto externo no usado al entrenar.

### 3.2. Alternativa interactiva: notebook con el mismo código

Para explorar o visualizar el proceso en Windows, crear un kernel aislado:

```powershell
python -m venv .venv-star
.\.venv-star\Scripts\python.exe -m pip install -r .\notebooks\requirements_star.txt
.\.venv-star\Scripts\python.exe -m ipykernel install --user --name mlops-star --display-name "Python (MLOps star)"
```

También puede usarse `py -3.12 -m venv .venv-star`. No es necesario activar el
entorno. En VS Code/Jupyter abrir `notebooks/experiment_star_mlflow.ipynb` y
seleccionar **Python (MLOps star)** o `.venv-star/Scripts/python.exe`.
Si falta JupyterLab:

```powershell
.\.venv-star\Scripts\python.exe -m pip install jupyterlab
.\.venv-star\Scripts\python.exe -m jupyterlab
```

Iniciar desde la raíz o `notebooks/`, reiniciar el kernel tras instalar paquetes y
ejecutar las celdas en orden. Las funciones se importan del mismo paquete utilizado
por el DAG; `notebooks/star_workflow.py` queda como adaptador de compatibilidad.
Se requieren MLflow 3.16.1 y scikit-learn 1.7.2, como en la web y el worker.

Con valores predeterminados no hace falta cambiar conexiones. La notebook no lee
`.env` automáticamente. Si cambian puertos o credenciales, configurar antes de su
celda de conexión:

```python
import os
os.environ["STAR_MLFLOW_URI"] = "http://127.0.0.1:5001"
os.environ["STAR_S3_ENDPOINT"] = "http://127.0.0.1:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
# Opcional para una prueba breve:
# os.environ["STAR_N_TRIALS"] = "2"
# os.environ["STAR_MIN_DELTA"] = "0"
```

Usar las credenciales del MinIO local, sin publicar credenciales personales.
La notebook tiene celdas distintas para buscar, entrenar, evaluar, registrar y
comparar/promover. Si una publicación falla, reintentar esa celda manteniendo
`search`, `trained`, `evaluated` o `candidate` en memoria, sin repetir Optuna.
Tras reiniciar el kernel esas variables se pierden; el DAG ofrece checkpoints
persistentes para ese caso. La celda final de inferencia carga el champion vigente,
que puede seguir siendo el modelo anterior si el candidato no lo mejoró.


## 4. Iniciar la web y predecir

Con MLflow, MinIO y `star_type_model_prod@champion` disponibles, ejecutar:

```powershell
.\iniciar-web.ps1
```

El script detecta un Python compatible, crea `.venv-star-web`, instala sus propias
dependencias y arranca la web en [http://localhost:8801](http://localhost:8801).
No requiere Anaconda ni activa el entorno de la notebook. Mantener esa terminal
abierta; **Ctrl+C** detiene la web.

Si Windows no permite ejecutar `.ps1`, no es necesario cambiar su política:

```powershell
python -m venv .venv-star-web
.\.venv-star-web\Scripts\python.exe -m pip install -r .\star_web\requirements.txt
.\.venv-star-web\Scripts\python.exe .\star_web\app.py
```

Para usar otro puerto, establecer antes de iniciar:

```powershell
$env:STAR_WEB_PORT = "8802"
```

La web consulta hasta 10000 objetos de SDSS. Cargar la lista, seleccionar un
`objid` y pulsar **Predecir tipo de objeto**. Envía bandas y colores sin escalar a
`POST /api/predict`; el Pipeline aplica el scaler. Después de predecir, compara
el resultado con la clase SDSS, oculta previamente en la pantalla.
La etiqueta visible es **Modelo de predicción**. La referencia SDSS viaja en los
datos de la página para esa comparación; no es un dato secreto.

La documentación de esta API está en [http://localhost:8801/docs](http://localhost:8801/docs).
La API de 8800 pertenece al ejemplo Heart Disease. La consulta web se cachea una
hora y el modelo un minuto; una actualización de `champion` puede tardar ese
minuto en reflejarse. Los objetos de la web pueden incluir datos de entrenamiento;
la comparación visual no es una evaluación independiente del modelo.

## 5. Certificados: solo si la red los requiere

En una conexión normal no hay pasos adicionales. Si aparece
`CERTIFICATE_VERIFY_FAILED`, un antivirus o proxy podría inspeccionar HTTPS.
No desactivar la validación TLS ni usar `--trusted-host` para resolverlo.

### Consultas SDSS y pip de Windows

El secreto del build **no configura HTTPS en tiempo de ejecución**. Si también
falla SDSS ya se generó el bundle validado con `preparar-ca.ps1`:

```powershell
if (-not (Test-Path .\airflow\config\sdss-ca-bundle.pem)) {
    Copy-Item .\airflow\config\pip-ca-bundle.pem .\airflow\config\sdss-ca-bundle.pem
}
```

El DAG y la web detectan ese archivo por defecto. La carpeta `airflow/config` está
montada en los contenedores; reintentar la tarea fallida o la consulta web. Si ya
hay un bundle propio para SDSS, conservarlo y validar cuál corresponde antes de
reemplazarlo. La notebook descarga desde MinIO local y no consulta SDSS directamente.

Si falla pip al instalar el entorno Windows, establecer en esa terminal antes de
repetir la instalación:

```powershell
$env:PIP_CERT = (Resolve-Path .\airflow\config\pip-ca-bundle.pem).Path
```

Estos archivos son locales, públicos y están excluidos de Git. No contienen claves
privadas ni modifican el almacén de confianza del sistema. Si la política impide
ejecutar el exportador, usar un bundle validado entregado por el administrador.
