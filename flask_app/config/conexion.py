from flask_mysqldb import MySQL

def con_mysql(app):
	app.secret_key = 'many random bytes'
	app.config['MYSQL_HOST'] = 'localhost'
	app.config['MYSQL_USER'] = 'root'
	app.config['MYSQL_PASSWORD'] = ''
	app.config['MYSQL_DB'] = ''
	mysql = MySQL(app)
	return mysql