#!/usr/bin/env bash
python cisco-grpc-dialin.py -s sr -u root -p lablab -a 52.52.52.1 -r 57400 -b 5 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s arp -u root -p lablab -a 52.52.52.1 -r 57400 -b 18 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s bgp -u root -p lablab -a 52.52.52.1 -r 57400 -b 87 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s cpu -u root -p lablab -a 52.52.52.1 -r 57400 -b 7 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s qos -u root -p lablab -a 52.52.52.1 -r 57400 -b 27 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s xtc -u root -p lablab -a 52.52.52.1 -r 57400 -b 6 -e 1.1.1.10 > /dev/null 2>&1 &
python cisco-grpc-dialin.py -s isis -u root -p lablab -a 52.52.52.1 -r 57400 -b 60 -e 1.1.1.10 > /dev/null 2>&1 &
