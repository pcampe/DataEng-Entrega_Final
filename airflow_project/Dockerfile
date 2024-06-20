FROM apache/airflow:2.3.3

# Instalar dependencias
RUN pip install requests pandas sqlalchemy psycopg2-binary python-dotenv

# Copiar DAGs y archivos al contenedor
COPY dags/ /opt/airflow/dags/
COPY .env /opt/airflow/.env

# Configurar entorno
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Inicializar la base de datos de Airflow
RUN airflow db init

# Crear usuario admin
RUN airflow users create -r Admin -u admin -p admin -e admin@example.com -f Admin -l User

# Ejecutar Airflow
CMD ["airflow", "scheduler"]
