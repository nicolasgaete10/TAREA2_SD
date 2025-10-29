#!/bin/bash

echo "=========================================="
echo "  VERIFICADOR DE CONFIGURACIÓN TAREA 2"
echo "=========================================="
echo ""

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Contadores
ERRORES=0
ADVERTENCIAS=0

# Función para verificar archivo
check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1 ${RED}(FALTA)${NC}"
        ((ERRORES++))
    fi
}

# Función para verificar directorio
check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✓${NC} $1/"
    else
        echo -e "${RED}✗${NC} $1/ ${RED}(FALTA)${NC}"
        ((ERRORES++))
    fi
}

echo "1. Verificando estructura de directorios..."
echo "-------------------------------------------"
check_dir "Data"
check_dir "almacenamiento"
check_dir "traffic_generator"
check_dir "procesador_llm"
check_dir "flink"
check_dir "almacenamiento-kafka"

echo ""
echo "2. Verificando archivos principales..."
echo "-------------------------------------------"
check_file "docker-compose.yml"
check_file ".env"
check_file "Data/test.csv"

echo ""
echo "3. Verificando servicio: almacenamiento..."
echo "-------------------------------------------"
check_file "almacenamiento/app_storage.py"
check_file "almacenamiento/Dockerfile"
check_file "almacenamiento/requirements.txt"

echo ""
echo "4. Verificando servicio: traffic_generator..."
echo "-------------------------------------------"
check_file "traffic_generator/app.py"
check_file "traffic_generator/Dockerfile"
check_file "traffic_generator/requirements.txt"

echo ""
echo "5. Verificando servicio: procesador_llm..."
echo "-------------------------------------------"
check_file "procesador_llm/app.py"
check_file "procesador_llm/Dockerfile"
check_file "procesador_llm/requirements.txt"

echo ""
echo "6. Verificando servicio: flink (NUEVO)..."
echo "-------------------------------------------"
check_file "flink/flink.py"
check_file "flink/Dockerfile"

echo ""
echo "7. Verificando servicio: almacenamiento-kafka (NUEVO)..."
echo "-------------------------------------------"
check_file "almacenamiento-kafka/kafkadata.py"
check_file "almacenamiento-kafka/Dockerfile"
check_file "almacenamiento-kafka/requirements.txt"

echo ""
echo "8. Verificando configuración de .env..."
echo "-------------------------------------------"
if [ -f ".env" ]; then
    if grep -q "GEMINI_API_KEY=" .env; then
        API_KEY=$(grep "GEMINI_API_KEY=" .env | cut -d'=' -f2)
        if [ -z "$API_KEY" ] || [ "$API_KEY" == "tu_api_key_aqui" ]; then
            echo -e "${YELLOW}⚠${NC} GEMINI_API_KEY está vacía o tiene valor de ejemplo"
            echo -e "  ${YELLOW}Recuerda configurar tu API key real${NC}"
            ((ADVERTENCIAS++))
        else
            echo -e "${GREEN}✓${NC} GEMINI_API_KEY configurada"
        fi
    else
        echo -e "${RED}✗${NC} GEMINI_API_KEY no encontrada en .env"
        ((ERRORES++))
    fi
else
    echo -e "${RED}✗${NC} Archivo .env no existe"
    ((ERRORES++))
fi

echo ""
echo "9. Verificando docker-compose.yml..."
echo "-------------------------------------------"
if [ -f "docker-compose.yml" ]; then
    if grep -q "flink-quality-checker" docker-compose.yml; then
        echo -e "${GREEN}✓${NC} Servicio flink-quality-checker configurado"
    else
        echo -e "${RED}✗${NC} Servicio flink-quality-checker NO encontrado en docker-compose.yml"
        ((ERRORES++))
    fi
    
    if grep -q "storage_consumer" docker-compose.yml || grep -q "storage-consumer" docker-compose.yml; then
        echo -e "${GREEN}✓${NC} Servicio storage_consumer configurado"
    else
        echo -e "${RED}✗${NC} Servicio storage_consumer NO encontrado en docker-compose.yml"
        ((ERRORES++))
    fi
else
    echo -e "${RED}✗${NC} docker-compose.yml no encontrado"
    ((ERRORES++))
fi

echo ""
echo "=========================================="
echo "         RESUMEN DE VERIFICACIÓN"
echo "=========================================="

if [ $ERRORES -eq 0 ] && [ $ADVERTENCIAS -eq 0 ]; then
    echo -e "${GREEN}✓ ¡Todo listo! El proyecto está correctamente configurado.${NC}"
    echo ""
    echo "Puedes ejecutar el sistema con:"
    echo "  docker-compose up --build"
    exit 0
elif [ $ERRORES -eq 0 ]; then
    echo -e "${YELLOW}⚠ Hay $ADVERTENCIAS advertencia(s), pero puedes continuar.${NC}"
    echo ""
    echo "Revisa las advertencias arriba antes de ejecutar."
    exit 0
else
    echo -e "${RED}✗ Hay $ERRORES error(es) que deben corregirse.${NC}"
    if [ $ADVERTENCIAS -gt 0 ]; then
        echo -e "${YELLOW}⚠ También hay $ADVERTENCIAS advertencia(s).${NC}"
    fi
    echo ""
    echo "Por favor, crea los archivos faltantes antes de continuar."
    exit 1
fi