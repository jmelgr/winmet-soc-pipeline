#!/bin/bash
set -e
export PYSPARK_PYTHON="./venv/bin/python"
export PYSPARK_DRIVER_PYTHON="./venv/bin/python"

echo "=================================================="
echo "[*] INICIANDO PIPELINE SOC END-TO-END"
echo "=================================================="

echo "======================="
echo "[+] Ingesta por lotes"
echo "======================="

./ingest_batches.sh

echo "=================================================="
echo "[+] Visualizar cómo te infiere tus schemas Spark"
echo "=================================================="

echo -n "[1/2] Ver esquema inferido de WinMET... "
spark-submit src/schemspark/winmet_inference_schema.py

echo -n "[2/2] Ver esquema inferido de Consensus... "
spark-submit --driver-memory 10g src/schemspark/consensus_inference_schema.py

echo "=================================================="
echo "[+] Schemas esperados (Contratos)"
echo "=================================================="

echo -n "[1/2] Contrato para WinMET... "
spark-submit src/contracts/winmet_contract_schema.py

echo -n "[2/2] Contrato para Consensus... "
spark-submit --driver-memory 10g src/contracts/consensus_contract_schema.py

echo "=================================================="
echo "[+] Limpieza de datos"
echo "=================================================="

echo -n "[1/2] Limpiando WinMET... "
spark-submit src/cleaning/winmet_clean.py

echo -n "[2/2] Limpiando Consensus... "
spark-submit --driver-memory 10g src/cleaning/consensus_clean.py

echo "=================================================="
echo "[+] Transformación de datos"
echo "=================================================="

echo -n "[1/2] Transformando WinMET... "
spark-submit src/transformation/winmet_transformation.py

echo -n "[2/2] Transformando Consensus... "
spark-submit --driver-memory 10g src/transformation/consensus_transformation.py

echo "=================================================="
echo "[+] Ingeniería de características e integración"
echo "=================================================="

echo -n "[1/1] Enriqueciendo... "
spark-submit src/enrichment/silver_enrichment.py

echo "=================================================="
echo "[*] Análisis"
echo "=================================================="

echo -n "[1/3] Top 10 familias de malware más frecuentes"
spark-submit analysis/top_10_family_malware/family.py

echo -n "[2/3] Distribución de muestras según complejidad de ejecución..."
spark-submit analysis/distribution_of_complex/complex.py

echo -n "[3/3] Top 15 procesos más frecuentes invocados..."
spark-submit analysis/top_15_process/top_process.py

echo ""
echo "=================================================="
echo "[✔] PIPELINE COMPLETADO EXITOSAMENTE"
echo "=================================================="
echo " Finalizado en: $(date '+%Y-%m-%d %H:%M:%S')"
echo " Arquitectura  : Medallion (Bronze -> Silver -> Enriched)"
echo " Artefactos    : Gráficos y reportes exportados en ./outputs/"
echo "=================================================="
