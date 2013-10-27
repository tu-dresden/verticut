.PHONY: clean

all:
	cd Pilaf/; make
	cd image_search/; make

clean:
	cd Pilaf/; make clean
	cd image_search/; make clean
