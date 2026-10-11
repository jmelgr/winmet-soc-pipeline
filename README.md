# Guía de Ejecución - SOC Malware Analysis Pipeline (Big Data)
¡Tienes que modificar las rutas del .env colocando tu user!
### 0. Instalar dependencias del sistema (7-zip)
```
sudo apt update && sudo apt install -y p7zip-full
```

### 1. Configurar el Entorno Virtual y Dependencias
```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
### 2. Iniciar servicios de Hadoop (HDFS y YARN)
```
start-dfs.sh
start-yarn.sh
jps
```
### 3. Configurar estructura inicial en HDFS
```
hdfs dfs -mkdir -p /user/$(whoami)/winmet_soc/raw
hdfs dfs -mkdir -p /user/$(whoami)/winmet_soc/bronze
```
### 4. Subir datos crudos a HDFS
- **Descargar** el volumen comprimido `WinMET_volume_1.7z` desde [Zenodo](https://zenodo.org/records/16414116) y colocarlo en la ruta local `data/raw/`.
- Subir los datos crudos a HDFS: 
```
hdfs dfs -put data/raw/reports_consensus_label.json data/raw/WinMET_volume_1.7z /user/$(whoami)/winmet_soc/raw/
```
### 5. Verificar carga en HDFS
```
hdfs dfs -ls -h /user/$(whoami)/winmet_soc/raw
```
### 6. Dar permisos de ejecución a los scripts
```
chmod +x ingest_batches.sh
chmod +x run_pipeline.sh
```
### 7. Ejecutar el Pipeline E2E
```
./run_pipeline.sh
```

### 8. Verificación de capas en HDFS
```
hdfs dfs -count /user/$(whoami)/winmet_soc/bronze
hdfs dfs -count /user/$(whoami)/winmet_soc/silver
```
