# our base image
FROM python:3-onbuild

# onbuild версия содержит помогаторы для развертывания приложения
# в этом случае она является альтернативой ручным set a directory for the app, copy all the files to the container, install dependencies
# FROM python:3
# WORKDIR /usr/src/app
# COPY . .
# RUN pip install --no-cache-dir -r requirements.txt

# tell the port number the container should expose
EXPOSE 5000

# run the command
CMD ["python", "./app.py"]
