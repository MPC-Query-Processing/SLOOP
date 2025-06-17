# SLOOP: Secure Linear Online cOmplexity query Processing system

We provide **SLOOP**: a semi-honest three-party honest majority implementation of "Secure Query Processing with Linear Online Cost" using the [ABY3](https://github.com/ladnir/aby3) framework.

## Installation

Please refer to `README-aby3.md` for building dependencies.
After download the project and enter the directory, type the following command lines to build the project
```
python3 build.py --setup
python3 build.py 
```

Our codes and OptScape are in `SMQP` (Secure Multi-party Query Processing) directory.

## Queries

Please refer to `queries.md` for all TPC-H queries and graph queries.

## Preprocessing
A simple demo to generate the ranks for TPC-H dataset.
```
cd bin 
./genranks [0/1/2] [1M/10M/100M/1G]
```
The ranks are pregenerated and concatenated at the end of each line in the raw data. Data with ranks are named as "xxx_pi.csv" in the data repo.

## Execution

For a simple demo, please open three terminals and execute
```
cd bin 
./tpch [0/1/2] [1M/10M/100M/1G]
./bitcoinalpha [0/1/2]
```
`[0/1/2]` represents the role of three servers.

`[1M/10M/100M/1G]` represents the scale size of TPC-H benchmark.

See an example of TPC-H Query 3 with 1M data.
```
[Terminal 1]            |[Terminal 2]         |[Terminal 3]           
./tpchq3 0 1M           |./tpchq3 1 1M        |./tpchq3 2 1M 
```

And the output (of server 0) is
```
total time cost = 0.393474
total comm cost = 7083718
7 960110 0 1649565556
36 951103 0 389889864
102 970509 0 1099238510
160 961219 0 833518280
166 950912 0 904417900
192 971125 0 1295583747
288 970221 0 1575161949
320 971121 0 395776259
390 980407 0 1612456420
455 961204 0 1314150753
...
```

And the time(s)/communication(Bytes) costs of server 1 & 2 are:
```
[Server 1]
total time cost = 0.397653
total comm cost = 8029020
[Server 2]
total time cost = 0.396853
total comm cost = 8071314
```

The first 10 lines on plaintext are exactly the same, except we use integers to represent fixed the fixed-point floats.
```
+------------+-------------+----------------+-----------------------------------------+
| o_orderkey | o_orderdate | o_shippriority | SUM(l_extendedprice * (1 - l_discount)) |
+------------+-------------+----------------+-----------------------------------------+
|          7 | 1996-01-10  |              0 |                             164956.5556 |
|         36 | 1995-11-03  |              0 |                              38988.9864 |
|        102 | 1997-05-09  |              0 |                             109923.8510 |
|        160 | 1996-12-19  |              0 |                              83351.8280 |
|        166 | 1995-09-12  |              0 |                              90441.7900 |
|        192 | 1997-11-25  |              0 |                             129558.3747 |
|        288 | 1997-02-21  |              0 |                             157516.1949 |
|        320 | 1997-11-21  |              0 |                              39577.6259 |
|        390 | 1998-04-07  |              0 |                             161245.6420 |
|        455 | 1996-12-04  |              0 |                             131415.0753 |
+------------+-------------+----------------+-----------------------------------------+
```