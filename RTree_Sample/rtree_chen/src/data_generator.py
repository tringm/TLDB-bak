from random import *

def writeToFileID(num_line,filename):
    file = open(filename, 'w')
    for i in range(num_line):
        file.write(str(i+1)+"\n")
    file.close()

def writeToFileRandom(num_line,filename):
    file = open(filename, 'w')
    for i in range(num_line):
        file.write(str(randint(1, 300000)/3000.0) + "\n")
    file.close()
def test_1():
    num_line = 229166
    filename = '../data/orderID_v.dat'
    writeToFileID(num_line,filename)
    return

def test_2():
    num_line = 229166
    filename = '../data/price_v.dat'
    writeToFileRandom(num_line, filename)
    return

if __name__ == "__main__":
    #tutorial()
    #test_kd()
    test_1()
    print("done")