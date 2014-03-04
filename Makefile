.PHONY: clean

all:
	cd Pilaf/; make
	cd Monkey/; make
	cd Redis/; make

clean:
	cd Pilaf/; make clean
	cd Monkey/; make clean
	cd Redis/; make clean
