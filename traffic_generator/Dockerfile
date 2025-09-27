# traffic_generator/Dockerfile

# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia el archivo de requerimientos primero
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación y los datos
COPY . .

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]