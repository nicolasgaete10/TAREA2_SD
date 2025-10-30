#!/bin/bash

# Script rápido para iniciar el sistema con configuración por defecto

echo "=========================================="
echo "  INICIO RÁPIDO - TAREA 2"
echo "=========================================="
echo ""

# Verificar .env
if [ ! -f ".env" ]; then
    echo "ERROR: No se encontró .env"
    echo "Crea uno con: echo 'GEMINI_API_KEY=tu_key' > .env"
    exit 1
fi

# Configuración por defecto
export TRAFFIC_DISTRIBUTION="uniform"
export SIMULATION_TIME_MIN=10
export QUALITY_THRESHOLD=0.3
export MAX_RETRIES=2

echo "Configuración por defecto:"
echo "  - Distribución: UNIFORME"
echo "  - Duración: 10 minutos"
echo "  - Umbral: 0.3"
echo "  - Reintentos: 2"
echo ""

# Detener contenedores previos
echo "🔧 Deteniendo contenedores anteriores..."
docker-compose down

echo ""
echo "🚀 Iniciando sistema..."
docker-compose up --build