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

# Exponer el puerto en el que correrá la aplicación
EXPOSE 5000

# Comando para ejecutar la aplicación usando un servidor WSGI como Gunicorn
# Es más robusto que el servidor de desarrollo de Flask para este caso
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]
