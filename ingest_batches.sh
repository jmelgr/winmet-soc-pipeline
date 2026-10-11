#!/bin/bash

RAW_ZIP="$HOME/BigData_UPAO/winmet_soc/data/raw/WinMET_volume_1.7z"
STAGING_DIR="$HOME/BigData_UPAO/winmet_soc/data/bronze_staging"
HDFS_TARGET="/user/$(whoami)/winmet_soc/bronze"
BATCH_SIZE=1000

mkdir -p "$STAGING_DIR"

echo "=== Iniciando Ingesta por Lotes a HDFS ==="

7z x "$RAW_ZIP" -o"$STAGING_DIR" -pinfected -y

ls "$STAGING_DIR"/*.json | xargs -n $BATCH_SIZE | while read -r batch; do
    echo "Subiendo lote de $(echo $batch | wc -w) archivos a HDFS..."
    hdfs dfs -put $batch "$HDFS_TARGET/"
    rm -f $batch
    echo "Lote subido y limpiado del disco local."
done

echo "=== Ingesta completada con éxito ==="
