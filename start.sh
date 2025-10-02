#!/bin/bash

echo "=========================================="
echo "  SISTEMA DE CACHE - SELECTOR DE PRUEBA"
echo "=========================================="
echo ""

echo "Paso 1: ¿Qué política de cache deseas usar?"
echo "1) FIFO - Puerto 5000"
echo "2) LRU - Puerto 5002"
read -p "Selecciona una opción (1-2): " cache_choice

echo ""

echo "Paso 2: ¿Qué distribución de tráfico deseas usar?"
echo "1) UNIFORME - Comportamiento predecible, baja localidad"
echo "2) EXPONENCIAL - Comportamiento realista, alta variabilidad/localidad"
read -p "Selecciona una opción (1-2): " traffic_choice


docker-compose down

case $traffic_choice in
    1)
        export TRAFFIC_DISTRIBUTION="uniform"
        echo "Configurando distribución: UNIFORME"
        ;;
    2)
        export TRAFFIC_DISTRIBUTION="exponential"
        echo "Configurando distribución: EXPONENCIAL"
        ;;
    *)
        echo "Opción de distribución inválida. Usando UNIFORME por defecto."
        export TRAFFIC_DISTRIBUTION="uniform"
        ;;
esac

echo ""

case $cache_choice in
    1)
        echo "Iniciando sistema con CACHE FIFO..."
        export CACHE_SERVICE_URL="http://cache_fifo:5000/check"
        # Iniciar todos los servicios
        docker-compose up --build cache_fifo cache_lru redis postgres almacenamiento score_service traffic_generator
        ;;
    2)
        echo "Iniciando sistema con CACHE LRU..."
        export CACHE_SERVICE_URL="http://cache_lru:5002/check"
        docker-compose up --build cache_fifo cache_lru redis postgres almacenamiento score_service traffic_generator
        ;;
    
    *)
        echo "Opción de caché inválida. Saliendo."
        exit 1
        ;;
esac
