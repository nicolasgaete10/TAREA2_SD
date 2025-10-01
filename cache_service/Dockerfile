# Usar una imagen base de Python ligera y moderna
FROM python:3.9-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de requerimientos primero para aprovechar el cache de Docker
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código de la aplicación
COPY . .

# Exponer los puertos (ambos servicios)
EXPOSE 5000 5002

# NO definir CMD aquí - se define en docker-compose