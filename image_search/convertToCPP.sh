#!/bin/bash
protoc -I=. --cpp_out=. image_search.proto
