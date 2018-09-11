#!/usr/bin/env bash
python cisco-grpc-dialin.py -s dial-in-mem -u root -p lablab -a 10.8.44.3 -r 57400 -e 1.1.1.2 > mem-info.log 2>&1 &
python cisco-grpc-dialin.py -s dial-in-ospf -u root -p lablab -a 10.8.44.3 -r 57400 -e 1.1.1.2 > ospf-info.log 2>&1 &
python cisco-grpc-dialin.py -s dial-in-show-ip-int-br -u root -p lablab -a 10.8.44.3 -r 57400 -e 1.1.1.2 > show-ip-int-br-info.log 2>&1 &
python cisco-grpc-dialin.py -s dial-in-bgp -u root -p lablab -a 10.8.44.3 -r 57400 -e 1.1.1.2 > bgp-info.log 2>&1 &
