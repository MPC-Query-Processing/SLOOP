/*
TPC-H Q3

============ Full Query ============ 
SELECT *
FROM
	customer, orders, lineitem
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey;

============ Join project Query ============ 
SELECT
    o_orderkey,
    o_orderdate,
    o_shippriority,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer, orders, lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_shipdate > '1995-03-15'
GROUP BY
    o_orderkey, o_orderdate, o_shippriority;

AND o_orderdate < '1995-03-15' (we omit this one ...)

*/

#include <iostream>
#include <vector>
#include <string>

#include <cryptoTools/Network/IOService.h>
#include "cryptoTools/Common/Timer.h"
#include "aby3/sh3/Sh3Runtime.h"
#include "aby3/sh3/Sh3Encryptor.h"
#include "aby3/sh3/Sh3Evaluator.h"
#include "aby3/sh3/Sh3BinaryEvaluator.h"
#include "aby3-DB/DBServer.h"

#include "Permutation.h"
#include "PrefixSumCircuit.h"
#include "join.h"
#include "SingleRelationOperator.h"
#include "baselinejoin.h"

using namespace oc;
using namespace aby3;

void DBSvrSetup(u64 partyIdx, IOService& ios, DBServer& srvs) {
    // PRNG prng(oc::ZeroBlock);
	if (partyIdx == 0) {
        PRNG prng(oc::toBlock(u64(0), u64(0)));
        Session s01(ios, "127.0.0.1:1111", SessionMode::Server, "01");
        Session s02(ios, "127.0.0.1:2222", SessionMode::Server, "02");
        srvs.init(0, s02, s01, prng);
    } else if (partyIdx == 1) {
        PRNG prng(oc::toBlock(u64(0), u64(1)));
        Session s10(ios, "127.0.0.1:1111", SessionMode::Client, "01");
        Session s12(ios, "127.0.0.1:3333", SessionMode::Server, "12");
        srvs.init(1, s10, s12, prng);
    } else {
        PRNG prng(oc::toBlock(u64(0), u64(2)));
        Session s20(ios, "127.0.0.1:2222", SessionMode::Client, "02");
    	Session s21(ios, "127.0.0.1:3333", SessionMode::Client, "12");
        srvs.init(2, s21, s20, prng);
    }
}

void LoadData(std::string filepath, std::vector< std::vector<uint64_t> > &table) {
    freopen(filepath.c_str(), "r", stdin);
    uint64_t val;
    char ch;
    std::vector<uint64_t> tuple;
    while (scanf("%llu%c", &val, &ch) != EOF) {
        tuple.push_back(val);
        if (ch == '\n') {
            table.push_back(tuple);
            tuple.clear();
        }
    }
    return;
}

void FullJoin(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and (c_mktsegment = 'BUILDING') AS c_tag
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_tag", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ3/" + scale + "/customer_pi.csv", raw_customer);
    u64 customer_nrows = raw_customer.size();
    Table customer(customer_nrows, customerColInfo);
    for (auto i=0; i<customer_nrows; ++i) {
        for (auto j=0; j<raw_customer[i].size(); ++j) {
            customer.mColumns[j].mData(i, 0) = raw_customer[i][j];
        }
    }
    SharedTable Scustomer;
    if (partyIdx == 0) {
        Scustomer = srvs.localInput(customer);
    } else {
        Scustomer = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared customer \n";
    // STableReveal(srvs, Scustomer, 10);
    // std::cerr << "\n";

    // o_custkey, o_orderkey, o_orderdate, o_shippriority,
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderdate", TypeID::IntID, 64 }, ColumnInfo{ "o_shippriority", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ3/" + scale + "/orders_pi.csv", raw_orders);
    u64 orders_nrows = raw_orders.size();
    Table orders(orders_nrows, ordersColInfo);
    for (auto i=0; i<orders_nrows; ++i) {
        for (auto j=0; j<raw_orders[i].size(); ++j) {
            orders.mColumns[j].mData(i, 0) = raw_orders[i][j];
        }
    }
    SharedTable Sorders;
    if (partyIdx == 0) {
        Sorders = srvs.localInput(orders);
    } else {
        Sorders = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared orders \n";
    // STableReveal(srvs, Sorders, 10);
    // std::cerr << "\n";

    // l_orderkey, (l_shipdate > '1995-03-15') AS l_tag, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ3/" + scale + "/lineitem_pi.csv", raw_lineitem);
    u64 lineitem_nrows = raw_lineitem.size();
    Table lineitem(lineitem_nrows, lineitemColInfo);
    for (auto i=0; i<lineitem_nrows; ++i) {
        for (auto j=0; j<raw_lineitem[i].size(); ++j) {
            lineitem.mColumns[j].mData(i, 0) = raw_lineitem[i][j];
        }
    }
    SharedTable Slineitem;
    if (partyIdx == 0) {
        Slineitem = srvs.localInput(lineitem);
    } else {
        Slineitem = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared lineitem \n";
    // STableReveal(srvs, Slineitem, 10);
    // std::cerr << "\n";

    std::cout << "Our Full Join Case \n";
    std::cout << "# of tuples = " << orders_nrows + lineitem_nrows + customer_nrows << std::endl;

    SharedTable SLOC, SOC;

    auto start_time = clock();
    PKJoin(partyIdx, srvs, Sorders, 0, -1, 4, Scustomer, 0, -1, 2, {}, SOC);
    // PKJoin(partyIdx, srvs, Sorders, 0, 1, 4, Scustomer, 0, 0, 2, {}, SOC);
    // o_custkey, o_orderkey, o_orderdate, o_shippriority, o_custkey_pi, o_orderkey_pi, c_custkey, c_tag, c_custkey_pi
    std::vector<u64> SOC_remain_col_lists = {1, 2, 3, 7, 5};
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    TakeColumns(SOC, SOC_remain_col_lists);
    auto end_time = clock();
    double OC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOC, 10);
    // std::cerr << "\n";

    start_time = clock();
    PKJoin(partyIdx, srvs, Slineitem, 0, -1, 3, SOC, 0, -1, 4, {}, SLOC);
    // PKJoin(partyIdx, srvs, Slineitem, 0, 0, 3, SOC, 0, -1, 4, {}, SLOC);
    // l_orderkey, l_tag, l_revenue, l_orderkey_pi, o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    std::vector<u64> SLOC_remain_col_lists = {1, 2, 5, 6, 7};
    // l_tag, l_revenue, o_orderdate, o_shippriority, c_tag
    TakeColumns(SLOC, SLOC_remain_col_lists);
    end_time = clock();
    double LOC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cerr << "LOC phase time cost = " << LOC_time << std::endl;

    // std::cerr << "\nshared LOC \n";
    // STableReveal(srvs, SLOC, 10);
    // std::cerr << "\n";

    std::cout << "total time cost = " << OC_time + LOC_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "result size = " << SLOC.rows() * SLOC_remain_col_lists.size() * 64 << std::endl << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 4 * 8 + SLOC.rows() * 5 * 8 << std::endl;
}

void FullJoinBaseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and (c_mktsegment = 'BUILDING') AS c_tag
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_tag", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ3/" + scale + "/customer_pi.csv", raw_customer);
    u64 customer_nrows = raw_customer.size();
    Table customer(customer_nrows, customerColInfo);
    for (auto i=0; i<customer_nrows; ++i) {
        for (auto j=0; j<raw_customer[i].size(); ++j) {
            customer.mColumns[j].mData(i, 0) = raw_customer[i][j];
        }
    }
    SharedTable Scustomer;
    if (partyIdx == 0) {
        Scustomer = srvs.localInput(customer);
    } else {
        Scustomer = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared customer \n";
    // STableReveal(srvs, Scustomer, 10);
    // std::cerr << "\n";

    // o_custkey, o_orderkey, o_orderdate, o_shippriority,
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderdate", TypeID::IntID, 64 }, ColumnInfo{ "o_shippriority", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ3/" + scale + "/orders_pi.csv", raw_orders);
    u64 orders_nrows = raw_orders.size();
    Table orders(orders_nrows, ordersColInfo);
    for (auto i=0; i<orders_nrows; ++i) {
        for (auto j=0; j<raw_orders[i].size(); ++j) {
            orders.mColumns[j].mData(i, 0) = raw_orders[i][j];
        }
    }
    SharedTable Sorders;
    if (partyIdx == 0) {
        Sorders = srvs.localInput(orders);
    } else {
        Sorders = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared orders \n";
    // STableReveal(srvs, Sorders, 10);
    // std::cerr << "\n";

    // l_orderkey, (l_shipdate > '1995-03-15') AS l_tag, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ3/" + scale + "/lineitem_pi.csv", raw_lineitem);
    u64 lineitem_nrows = raw_lineitem.size();
    Table lineitem(lineitem_nrows, lineitemColInfo);
    for (auto i=0; i<lineitem_nrows; ++i) {
        for (auto j=0; j<raw_lineitem[i].size(); ++j) {
            lineitem.mColumns[j].mData(i, 0) = raw_lineitem[i][j];
        }
    }
    SharedTable Slineitem;
    if (partyIdx == 0) {
        Slineitem = srvs.localInput(lineitem);
    } else {
        Slineitem = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared lineitem \n";
    // STableReveal(srvs, Slineitem, 10);
    // std::cerr << "\n";

    std::cout << "Baseline Full Join Case \n";
    std::cout << "# of tuples = " << orders_nrows + lineitem_nrows + customer_nrows << std::endl;

    SharedTable SLOC, SOC;

    auto start_time = clock();
    PKJoinBaseline(partyIdx, srvs, Sorders, 0, -1, 0, Scustomer, 0, -1, 0, {}, SOC);
    // PKJoinBaseline(partyIdx, srvs, Sorders, 0, 1, 4, Scustomer, 0, 0, 2, {}, SOC);
    // o_custkey, o_orderkey, o_orderdate, o_shippriority, o_custkey_pi, o_orderkey_pi, c_custkey, c_tag, c_custkey_pi
    std::vector<u64> SOC_remain_col_lists = {1, 2, 3, 7};
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    TakeColumns(SOC, SOC_remain_col_lists);
    auto end_time = clock();
    double OC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOC, 10);
    // std::cerr << "\n";

    start_time = clock();
    PKJoinBaseline(partyIdx, srvs, Slineitem, 0, -1, 0, SOC, 0, -1, 0, {}, SLOC);
    // PKJoinBaseline(partyIdx, srvs, Slineitem, 0, 0, 3, SOC, 0, -1, 4, {}, SLOC);
    // l_orderkey, l_tag, l_revenue, l_orderkey_pi, o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    std::vector<u64> SLOC_remain_col_lists = {1, 2, 5, 6, 7};
    // l_tag, l_revenue, o_orderdate, o_shippriority, c_tag
    TakeColumns(SLOC, SLOC_remain_col_lists);
    end_time = clock();
    double LOC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "LOC phase time cost = " << LOC_time << std::endl;
    std::cout << "total time cost = " << OC_time + LOC_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "result size = " << SLOC.rows() * SLOC_remain_col_lists.size() * 64 << std::endl << std::endl;

    // std::cerr << "\nshared LOC \n";
    // STableReveal(srvs, SLOC, 10);
    // std::cerr << "\n";
}

void JoinProject(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and (c_mktsegment = 'BUILDING') AS c_tag
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_tag", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ3/" + scale + "/customer_pi.csv", raw_customer);
    u64 customer_nrows = raw_customer.size();
    Table customer(customer_nrows, customerColInfo);
    for (auto i=0; i<customer_nrows; ++i) {
        for (auto j=0; j<raw_customer[i].size(); ++j) {
            customer.mColumns[j].mData(i, 0) = raw_customer[i][j];
        }
    }
    SharedTable Scustomer;
    if (partyIdx == 0) {
        Scustomer = srvs.localInput(customer);
    } else {
        Scustomer = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared customer \n";
    // STableReveal(srvs, Scustomer, 10);
    // std::cerr << "\n";

    // o_custkey, o_orderkey, o_orderdate, o_shippriority,
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderdate", TypeID::IntID, 64 }, ColumnInfo{ "o_shippriority", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ3/" + scale + "/orders_pi.csv", raw_orders);
    u64 orders_nrows = raw_orders.size();
    Table orders(orders_nrows, ordersColInfo);
    for (auto i=0; i<orders_nrows; ++i) {
        for (auto j=0; j<raw_orders[i].size(); ++j) {
            orders.mColumns[j].mData(i, 0) = raw_orders[i][j];
        }
    }
    SharedTable Sorders;
    if (partyIdx == 0) {
        Sorders = srvs.localInput(orders);
    } else {
        Sorders = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared orders \n";
    // STableReveal(srvs, Sorders, 10);
    // std::cerr << "\n";

    // l_orderkey, (l_shipdate > '1995-03-15') AS l_tag, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ3/" + scale + "/lineitem_pi.csv", raw_lineitem);
    u64 lineitem_nrows = raw_lineitem.size();
    Table lineitem(lineitem_nrows, lineitemColInfo);
    for (auto i=0; i<lineitem_nrows; ++i) {
        for (auto j=0; j<raw_lineitem[i].size(); ++j) {
            lineitem.mColumns[j].mData(i, 0) = raw_lineitem[i][j];
        }
    }
    SharedTable Slineitem;
    if (partyIdx == 0) {
        Slineitem = srvs.localInput(lineitem);
    } else {
        Slineitem = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared lineitem \n";
    // STableReveal(srvs, Slineitem, 10);
    // std::cerr << "\n";

    // std::cout << "Our Join Project Case \n";
    // std::cout << "# of tuples = " << orders_nrows + lineitem_nrows + customer_nrows << std::endl;

    SharedTable SLOC, SOC;
    auto start_time = clock();
    SemiJoin(partyIdx, srvs, Sorders, 0, -1, 4, Scustomer, 0, -1, 2, 1, AggFunc::SUM, SOC);
    // SemiJoin(partyIdx, srvs, Sorders, 0, 1, 4, Scustomer, 0, 0, 2, 1, AggFunc::SUM, SOC);
    // o_custkey, o_orderkey, o_orderdate, o_shippriority, o_custkey_pi, o_orderkey_pi, c_tag
    std::vector<u64> SOC_remain_col_lists = {1, 2, 3, 6, 5};
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    TakeColumns(SOC, SOC_remain_col_lists);
    auto end_time = clock();
    double OC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOC, 10);
    // std::cerr << "\n";

    start_time = clock();
    ColBitAnd(partyIdx, srvs, Slineitem.mColumns[1], Slineitem.mColumns[2]);
    end_time = clock();
    double self_mult_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "Self mult time cost = " << self_mult_time << std::endl;
    // std::cout << "total time cost = " << OC_time + self_mult_time << std::endl;

    start_time = clock();
    SemiJoin(partyIdx, srvs, SOC, 0, -1, 4, Slineitem, 0, -1, 3, 2, AggFunc::SUM, SLOC);
    // SemiJoin(partyIdx, srvs, SOC, 0, -1, 4, Slineitem, 0, 0, 3, 2, AggFunc::SUM, SLOC);
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi, l_revenue
    ColBitAnd(partyIdx, srvs, SLOC.mColumns[3], SLOC.mColumns[5]);
    std::vector<u64> SLOC_remain_col_lists = {0, 1, 2, 5};
    // l_tag, l_revenue, o_orderdate, o_shippriority, c_tag
    TakeColumns(SLOC, SLOC_remain_col_lists);
    end_time = clock();
    double LOC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "LOC phase time cost = " << LOC_time << std::endl;
    std::cout << "total time cost = " << OC_time + LOC_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    
    // std::cout << "IN+OUT = " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 4 * 8 + SLOC.rows() * 4 * 8 << std::endl;
    // CompactionZeroEntity(partyIdx, srvs, SLOC, 3);

    // std::cerr << "\nshared LOC \n";
    // STableReveal(srvs, SLOC);
    // std::cerr << "\n";
}

void JoinProjectBaseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and (c_mktsegment = 'BUILDING') AS c_tag
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_tag", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ3/" + scale + "/customer_pi.csv", raw_customer);
    u64 customer_nrows = raw_customer.size();
    Table customer(customer_nrows, customerColInfo);
    for (auto i=0; i<customer_nrows; ++i) {
        for (auto j=0; j<raw_customer[i].size(); ++j) {
            customer.mColumns[j].mData(i, 0) = raw_customer[i][j];
        }
    }
    SharedTable Scustomer;
    if (partyIdx == 0) {
        Scustomer = srvs.localInput(customer);
    } else {
        Scustomer = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared customer \n";
    // STableReveal(srvs, Scustomer, 10);
    // std::cerr << "\n";

    // o_custkey, o_orderkey, o_orderdate, o_shippriority,
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderdate", TypeID::IntID, 64 }, ColumnInfo{ "o_shippriority", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ3/" + scale + "/orders_pi.csv", raw_orders);
    u64 orders_nrows = raw_orders.size();
    Table orders(orders_nrows, ordersColInfo);
    for (auto i=0; i<orders_nrows; ++i) {
        for (auto j=0; j<raw_orders[i].size(); ++j) {
            orders.mColumns[j].mData(i, 0) = raw_orders[i][j];
        }
    }
    SharedTable Sorders;
    if (partyIdx == 0) {
        Sorders = srvs.localInput(orders);
    } else {
        Sorders = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared orders \n";
    // STableReveal(srvs, Sorders, 10);
    // std::cerr << "\n";

    // l_orderkey, (l_shipdate > '1995-03-15') AS l_tag, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ3/" + scale + "/lineitem_pi.csv", raw_lineitem);
    u64 lineitem_nrows = raw_lineitem.size();
    Table lineitem(lineitem_nrows, lineitemColInfo);
    for (auto i=0; i<lineitem_nrows; ++i) {
        for (auto j=0; j<raw_lineitem[i].size(); ++j) {
            lineitem.mColumns[j].mData(i, 0) = raw_lineitem[i][j];
        }
    }
    SharedTable Slineitem;
    if (partyIdx == 0) {
        Slineitem = srvs.localInput(lineitem);
    } else {
        Slineitem = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared lineitem \n";
    // STableReveal(srvs, Slineitem, 10);
    // std::cerr << "\n";
    std::cout << "Baseline Join Project Case \n";
    std::cout << "# of tuples = " << orders_nrows + lineitem_nrows + customer_nrows << std::endl;

    SharedTable SLOC, SOC;
    auto start_time = clock();
    SemiJoinBaseline(partyIdx, srvs, Sorders, 0, -1, 0, Scustomer, 0, -1, 0, 1, AggFunc::SUM, SOC);
    // SemiJoinBaseline(partyIdx, srvs, Sorders, 0, 1, 4, Scustomer, 0, 0, 2, 1, AggFunc::SUM, SOC);
    // o_custkey, o_orderkey, o_orderdate, o_shippriority, o_custkey_pi, o_orderkey_pi, c_tag
    std::vector<u64> SOC_remain_col_lists = {1, 2, 3, 6, 5};
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi
    TakeColumns(SOC, SOC_remain_col_lists);
    auto end_time = clock();
    double OC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOC, 10);
    // std::cerr << "\n";

    start_time = clock();
    ColBitAnd(partyIdx, srvs, Slineitem.mColumns[1], Slineitem.mColumns[2]);
    end_time = clock();
    double self_mult_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "Self mult time cost = " << self_mult_time << std::endl;
    std::cout << "total time cost = " << OC_time + self_mult_time << std::endl;

    start_time = clock();
    SemiJoinBaseline(partyIdx, srvs, SOC, 0, -1, 0, Slineitem, 0, -1, 0, 2, AggFunc::SUM, SLOC);
    // SemiJoinBaseline(partyIdx, srvs, SOC, 0, -1, 4, Slineitem, 0, 0, 3, 2, AggFunc::SUM, SLOC);
    // o_orderkey, o_orderdate, o_shippriority, c_tag, o_orderkey_pi, l_revenue
    ColBitAnd(partyIdx, srvs, SLOC.mColumns[3], SLOC.mColumns[5]);
    std::vector<u64> SLOC_remain_col_lists = {0, 1, 2, 5};
    // l_tag, l_revenue, o_orderdate, o_shippriority, c_tag
    TakeColumns(SLOC, SLOC_remain_col_lists);
    end_time = clock();
    double LOC_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "LOC phase time cost = " << LOC_time << std::endl;
    std::cout << "total time cost = " << OC_time + LOC_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    
    // STableReveal(srvs, SLOC);
    // CompactionZeroEntity(partyIdx, srvs, SLOC, 3);

    // std::cerr << "\nshared LOC \n";
    // STableReveal(srvs, SLOC);
    // std::cerr << "\n";
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    // FullJoinBaseline(partyIdx, scale);
    // FullJoin(partyIdx, scale);
    // JoinProjectBaseline(partyIdx, scale);
    JoinProject(partyIdx, scale);
    return 0;
}