import numpy
import random
import pickle

n_asin = 229166
n_Orderline = 229166
n_price = 229166

range_asin = [0, 300000]
range_Orderline = [100000, 400000]
range_price = [500000, 900000]

n_matches = 15

asin_v = random.sample(range(range_asin[0], range_asin[1]), n_asin)
Orderline_v = random.sample(range(range_Orderline[0], range_Orderline[1]), n_Orderline)
price_v = random.sample(range(range_price[0], range_price[1]), n_price)

n_Orderline_asin_table = 200000
Orderline_asin_table = []
for i in range(n_Orderline_asin_table - n_matches):
	orderline = random.randint(range_Orderline[0], range_Orderline[1])

	try:
		index = Orderline_v.index(orderline)
	except ValueError as e:
		index = -1

	asin = random.randint(range_asin[0], range_asin[1])

	if index != -1:
		while asin == asin_v[index]:
			asin = random.randint(range_asin[0], range_asin[1])

	Orderline_asin_table.append((orderline, asin))

n_price_asin_table = 200000
price_asin_table = []
for i in range(n_price_asin_table - n_matches):
	price = random.randint(range_price[0], range_price[1])

	try:
		index = price_v.index(price)
	except ValueError as e:
		index = -1

	while index >= len(asin_v):
		price = random.randint(range_price[0], range_price[1])

		try:
			index = price_v.index(price)
		except ValueError as e:
			index = -1

	asin = random.randint(range_asin[0], range_asin[1])

	if index != -1:
		while asin == asin_v[index]:
			asin = random.randint(range_asin[0], range_asin[1])

	price_asin_table.append((price, asin))

for i in range(n_matches):
	index = random.randint(0, min(len(price_v), len(asin_v), len(Orderline_v)))
	Orderline_asin_table.append((Orderline_v[index], asin_v[index]))
	price_asin_table.append((price_v[index], asin_v[index]))

with open('Orderline_asin_table.dat', 'w') as outfile:
	# pickle.dump(Orderline_asin_table, outfile)
	outfile.write('\n'.join(str(orderline) + " " + str(asin) for orderline, asin in Orderline_asin_table))

with open('price_asin_table.dat', 'w') as outfile:
	# pickle.dump(Orderline_asin_table, outfile)
	outfile.write('\n'.join(str(price) + " " + str(asin) for price, asin in price_asin_table))

with open('asin_v.dat', 'w') as outfile:
	outfile.write('\n'.join(str(asin) for asin in asin_v))
# pickle.dump(asin_v, outfile)

with open('Orderline_v.dat', 'w') as outfile:
	outfile.write('\n'.join(str(orderline) for orderline in Orderline_v))
# pickle.dump(Orderline_v, outfile)

with open('price_v.dat', 'w') as outfile:
	outfile.write('\n'.join(str(price) for price in price_v))
