.PHONY: clean

all:
	cd Pilaf/; make
	cd src/; make
	cd Redis/; make

clean:
	cd Pilaf/; make clean
	cd src/; make clean
	cd Redis/; make clean
