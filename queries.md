# Queries

## TPC-H
### All ranks
1. o_orderkey
2. o_custkey
3. l_orderkey
4. c_custkey
5. c_nationkey
6. n_nationkey
7. n_regionkey
8. r_regionkey
9. s_suppkey
10. l_suppkey
11. ps_suppkey
12. ps_partkey
13. s_nationkey

### Q3
```sql
SELECT o_orderkey, o_orderdate, o_shippriority,
       SUM(l_extendedprice * (1 - l_discount))
  FROM customer, orders, lineitem
 WHERE c_mktsegment = 'BUILDING'
   AND c_custkey = o_custkey
   AND l_orderkey = o_orderkey
   AND l_shipdate > '1995-03-15'
GROUP BY o_orderkey, o_orderdate, o_shippriority;
```
*ranks*
1. o_orderkey, o_orderdate, o_shippriority [o_orderkey (PK) is enough]
2. o_custkey
3. l_orderkey
4. c_custkey

### Q3F
```sql
SELECT *
  FROM customer, orders, lineitem
 WHERE c_custkey = o_custkey
   AND l_orderkey = o_orderkey;
```
*ranks*
1. o_orderkey
2. o_custkey
3. l_orderkey
4. c_custkey

### Q5F
```sql
SELECT *
  FROM lineitem, orders, customer, nation, region, supplier
 WHERE l_orderkey = o_orderkey
   AND o_custkey = c_custkey
   AND c_nationkey = n_nationkey
   AND n_regionkey = r_regionkey
   AND s_suppkey = l_suppkey;
```
*ranks*
1. o_orderkey
2. o_custkey
3. l_orderkey
4. c_custkey
5. c_nationkey
6. n_nationkey
7. n_regionkey
8. r_regionkey
9. s_suppkey
10. l_suppkey

### Q10
```sql
SELECT c_custkey, c_name, c_nationkey, sum(l_extendedprice*(1-l_discount))revenue
  FROM customer, orders, lineitem
 WHERE c_custkey = o_custkey
   AND l_orderkey = o_orderkey
   AND o_orderdate >= date '1993-08-01'
   AND o_orderdate < date '1993-11-01'
   AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_nationkey;
```
*ranks*
1. o_orderkey
2. o_custkey
3. l_orderkey
4. c_custkey, c_name, c_nationkey [c_custkey (PK) is enough]

### Q11
```sql
SELECT ps_partkey, SUM(ps_supplycost * ps_availqty)
  FROM partsupp, supplier, nation
 WHERE ps_suppkey = s_suppkey
   AND s_nationkey = n_nationkey
   AND n_name = 'ARGENTINA'
GROUP BY ps_partkey;
```
*ranks*
1. ps_suppkey
2. s_suppkey
3. s_nationkey
4. n_nationkey
5. ps_partkey

### Q18
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
  FROM customer, orders, lineitem
 WHERE o_orderkey IN (
    SELECT l_orderkey
      FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300)
   AND c_custkey = o_custkey
   AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice;
```
*ranks*
1. c_custkey
2. o_custkey
3. o_orderkey
4. l_orderkey
5. c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice [c_custkey is enough: c_custkey (PK) and o_orderkey (PK) = c_orderkey]

## Graph

### All ranks
1. target
2. source

### L3
```sql
SELECT b1.source, b1.target, b2.target, b3.target
  FROM bitcoin as b1, bitcoin as b2, bitcoin as b3
 WHERE b1.target = b2.source
   AND b2.target = b3.source
   AND b1.rating >= k
   AND b2.rating >= k
   AND b3.rating >= k;
```

### L3C
```sql
SELECT COUNT(*)
  FROM bitcoin as b1, bitcoin as b2, bitcoin as b3
 WHERE b1.target = b2.source
   AND b2.target = b3.source
   AND b1.rating >= k
   AND b2.rating >= k
   AND b3.rating >= k;
```