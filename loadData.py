import sqlite3 as sqli
from sqlite3 import Error
import pandas as pd


def load_data():
	in_df = pd.read_excel('20210309_2020_1 - 4.xls')[
		['API WELL  NUMBER', 'OIL', 'GAS', 'BRINE']]
	for well in in_df['API WELL  NUMBER'].unique():
		x = in_df[in_df['API WELL  NUMBER'] == well]
		totals_dict[well] = [x['OIL'].sum(), x['GAS'].sum(), x['BRINE'].sum()]


def create_sqlitable():
	""" create a database connection to a SQLite database """
	connection = None
	try:
		connection = sqli.connect("pythonsqlite.db")
	except Error as e:
		print(e)
	finally:
		if connection:
			cursor = connection.cursor()
			cursor.execute("""CREATE TABLE IF NOT EXISTS
			wells(well_id TEXT PRIMARY KEY, oil INTEGER, gas INTEGER, brine INTEGER)""")
			for key,value in totals_dict.items():
				cursor.execute("INSERT INTO wells VALUES ('{}', {}, {}, {})".format(key,value[0],value[1],value[2]))
			connection.commit()
			connection.close()

if __name__ == '__main__':
	totals_dict = {}
	load_data()
	create_sqlitable()
