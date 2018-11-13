# ===============
# 03 - DAC / USD Trades
# Get all completed trades DAC / USD for last 12 hours,
# check if key exists (txid) , if doesn't add new data to new row in table.
# Set script up to run as cron, cron to run every two hours (0 */2 * * *),
# Currently not much trading, when volume increase cron timings should be changed.
# ===============

import requests
import json
import urllib
import MySQLdb

API_URL = 'https://exchange.dacxi.com/api/exchange-trades?currencyPair=DAC-USD'

response = requests.get(API_URL).json()

db = MySQLdb.connect("localhost","dbUsername","dbPassword","dbName")

cursor = db.cursor()

for data in response['data']:

	var_txid = 			data['id']
	var_pair = 			'DAC/USD'
	var_side = 			data['side']
	var_amount = 		data['amount']
	var_price = 		data['price']
	var_total = 		data['total']
	var_created_at = 	data['created_at']

	cursor.execute("""INSERT INTO trades_dac_usd (id, txid, currency_pair, side, amount, price, total, created_at)
		VALUES (null, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE currency_pair=%s, side=%s, amount=%s, price=%s, total=%s, created_at=%s """,
		(var_txid, var_pair, var_side, var_amount, var_price, var_total, var_created_at, var_pair, var_side, var_amount, var_price, var_total, var_created_at))

	db.commit()

db.close()