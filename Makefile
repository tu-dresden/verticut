.PHONY: clean

all:
	cd Pilaf/; make
	cd image_search/; make
	cd Redis/; make

clean:
	cd Pilaf/; make clean
	cd image_search/; make clean
	cd Redis/; make clean
