FROM python:3.8

WORKDIR /oasis_app
COPY ./requirements.txt ./requirements.txt

# configurar ambiente de mi docker 
RUN python -m pip install --upgrade pip
# instalar librerías
RUN pip install --no-cache-dir -r requirements.txt 
RUN echo "termina instalación de librerías, empieza instalación de orca"
RUN pip install orca
RUN echo "termina instalación de CONDA inicia instalación de Git"
#no estoy seguro si la distribución ya incluye git
RUN apt-get -y install git
RUN apt-get install locales -y
RUN locale-gen es_ES.UTF-8
RUN echo "LANG=es_ES.UTF-8" > /etc/default/locale

#clonar reppositorio en el ambiente de trabajo
# Warning! Anyone who gets their hands on this image will be able
# to retrieve this private key file from the corresponding image layer
# RUN mkdir /root/.ssh/
# ADD id_rsa /root/.ssh/id_rsa
# Create known_hosts
# RUN touch /root/.ssh/known_hosts

# RUN ssh-keyscan -T 60 gitlab.com >> /root/.ssh/known_hosts

# RUN git clone git@gitlab.com:nutresa_oasis/full_oasis_lab.git

COPY . .

#cambiar de directorio
WORKDIR code

CMD ["python", "main_launcher.py"]