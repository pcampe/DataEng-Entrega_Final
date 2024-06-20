import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()  # Cargar variables de entorno desde el archivo .env

def send_email(subject, body):
    email_host = os.getenv('EMAIL_HOST')
    email_port = os.getenv('EMAIL_PORT')
    email_user = os.getenv('EMAIL_USER')
    email_password = os.getenv('EMAIL_PASSWORD')
    to_email = email_user  # Enviar a sí mismo, puedes cambiarlo según sea necesario

    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(email_host, email_port)
    server.starttls()
    server.login(email_user, email_password)
    text = msg.as_string()
    server.sendmail(email_user, to_email, text)
    server.quit()

def check_alerts():
    # Conectar a la base de datos
    engine = create_engine('postgresql://user:password@host:port/dbname')
    df = pd.read_sql('SELECT * FROM combined_data', engine)

    # Verificar si la temperatura supera el umbral
    temperature_threshold = 30  # Umbral de temperatura
    alerts = df[df['temp_c'] > temperature_threshold]

    if not alerts.empty:
        subject = 'Alerta de Temperatura Alta'
        body = 'Las siguientes ciudades tienen temperaturas superiores al umbral:\n\n'
        for index, row in alerts.iterrows():
            body += f"Ciudad: {row['city']}, Temperatura: {row['temp_c']}°C\n"
        send_email(subject, body)

if __name__ == "__main__":
    check_alerts()
