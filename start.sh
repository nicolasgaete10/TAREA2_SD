#!/bin/bash

echo "=========================================="
echo "  SISTEMA DE CACHE - SELECTOR DE MÉTRICA"
echo "=========================================="
echo ""
echo "¿Qué métrica de cache deseas usar?"
echo "1) FIFO (First-In-First-Out) - Puerto 5000"
echo "2) LRU (Least Recently Used) - Puerto 5002" 
echo ""
read -p "Selecciona una opción (1-2): " choice

docker-compose down

case $choice in
    1)
        echo "🔵 Iniciando sistema con CACHE FIFO..."
        # Configurar environment para FIFO
        export CACHE_SERVICE_URL="http://cache_fifo:5000/check"
        docker-compose up --build cache_fifo cache_lru redis postgres almacenamiento score_service traffic_generator
        ;;
    2)
        echo "🟢 Iniciando sistema con CACHE LRU..."
        # Configurar environment para LRU  
        export CACHE_SERVICE_URL="http://cache_lru:5002/check"
        docker-compose up --build cache_fifo cache_lru redis postgres almacenamiento score_service traffic_generator
        ;;
   
    *)
        echo "❌ Opción inválida. Saliendo."
        exit 1
        ;;
esac