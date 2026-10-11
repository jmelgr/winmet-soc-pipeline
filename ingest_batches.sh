#!/bin/bash

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_ZIP="$REPO_ROOT/data/raw/WinMET_volume_1.7z"
STAGING_DIR="$REPO_ROOT/data/bronze_staging"
HDFS_TARGET="/user/$(whoami)/winmet_soc/bronze"
BATCH_SIZE=1000

mkdir -p "$STAGING_DIR"

echo "=== Iniciando Ingesta por Lotes a HDFS ==="

if [ ! -f "$RAW_ZIP" ]; then
    echo "Error: No se encuentra el archivo comprimido en $RAW_ZIP"
    exit 1
fi

7z x "$RAW_ZIP" -o"$STAGING_DIR" -pinfected -y

find "$STAGING_DIR" -maxdepth 1 -name "*.json" | xargs -n $BATCH_SIZE | while read -r batch; do
    if [ -n "$batch" ]; then
        echo "Subiendo lote de archivos a HDFS..."
        hdfs dfs -put $batch "$HDFS_TARGET/"
        rm -f $batch
        echo "Lote subido y limpiado del disco local."
    fi
done

echo "=== Ingesta completada con éxito ==="
