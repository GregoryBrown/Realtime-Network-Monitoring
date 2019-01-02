#!/usr/bin/env bash
python cisco-grpc-dialin.py -s sr -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s arp -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s bgp -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s cpu -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s qos -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s xtc -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
python cisco-grpc-dialin.py -s isis -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s lacp -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s lldp -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s mpls -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s rsvp -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s memory -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s optics -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s ipv6-nd -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
#python cisco-grpc-dialin.py -s interface -u root -p lablab -a 10.8.61.5 -r 57400 -b 50 -e web-ott-tsdb-server > /dev/null &
