from collections import OrderedDict
import sqlite3 as sqli
from sqlite3 import Error
from flask import Flask, request

app = Flask(__name__)

@app.route('/data')
def get_record():
	well_id = request.args.get('well')
	connection = None
	try:
		connection = sqli.connect("pythonsqlite.db")
	except Error as e:
		print(e)
	finally:
		if connection:
			cursor = connection.cursor()
			cursor.execute("SELECT oil,gas,brine FROM wells WHERE well_id = '{}'".format(well_id))
			fetched_record = cursor.fetchone()
			connection.close()
			ret = OrderedDict([("oil",fetched_record[0]), ("gas",fetched_record[1]), ("brine",fetched_record[2])])
			return ret

if __name__ == '__main__':
	app.run(debug=False, port=8080)
