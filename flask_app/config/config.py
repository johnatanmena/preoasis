import os

class Config(object):
	SECRET_KEY = 'my_secret_key'
	MAIL_SERVER = 'smtp.gmail.com'	
	MAIL_PORT = 465
	MAIL_USER_SSL = False
	MAIL_USE_TLS = True
	MAIL_USERNAME = ''
	MAIL_PASSWORD = '*' #os.environ.get('PASSWORD_EMAIL_CF')


class DevelopmentConfig(Config):
	#todas las configuraciones del entrono del desarrollo
	DEBUG = True