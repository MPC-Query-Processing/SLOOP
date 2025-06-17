/*

select c_custkey, c_nationkey, sum(l_extendedprice*(1-l_discount))revenue
from customer, orders, lineitem
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-08-01'
  and o_orderdate < date '1993-11-01'
group by c_custkey, c_name, c_nationkey;

customer: c_custkey, c_name, c_nationkey, c_custkey_pi
orders: o_custkey, o_orderkey, o_orderflag, o_custkey_pi, o_orderkey_pi
lineitem: l_orderkey, l_revenue, l_orderkey_pi

SELECT c_custkey, c_nationkey FROM customer INTO OUTFILE "~/customer.csv" FIELDS TERMINATED BY "|" OPTIONALLY ENCLOSED BY "" LINES TERMINATED BY "\n" ;

SELECT o_custkey, o_orderkey, (o_orderdate > '1993-08-01' and o_orderdate < '1993-11-01') AS o_orderflag FROM orders INTO OUTFILE "~/orders.csv" FIELDS TERMINATED BY "|" OPTIONALLY ENCLOSED BY "" LINES TERMINATED BY "\n" ;

SELECT l_orderkey, convert((l_extendedprice * (1 - l_discount)*10000), SIGNED) AS l_revenue FROM lineitem INTO OUTFILE "~/lineitem.csv" FIELDS TERMINATED BY "|" OPTIONALLY ENCLOSED BY "" LINES TERMINATED BY "\n" ;


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


void Q10(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and c_nationkey
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ10/" + scale + "/customer_pi.csv", raw_customer);
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

    // o_custkey, o_orderkey, o_orderflag
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderflag", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ10/" + scale + "/orders_pi.csv", raw_orders);
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

    // l_orderkey, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ10/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    SharedTable SCOL, SOL;
    auto start_time = clock();
    SemiJoin(partyIdx, srvs, Sorders, 1, -1, 4, Slineitem, 0, -1, 2, 1, AggFunc::SUM, SOL);
    // o_custkey, o_orderkey, o_orderflag, o_custkey_pi, o_orderkey_pi, l_sum
    ColBitAnd(partyIdx, srvs, SOL.mColumns[2], SOL.mColumns[5]);
    std::vector<u64> SOL_remain_col_lists = {0, 5, 3};
    // o_custkey, o_tag_sum, o_custkey_pi
    TakeColumns(SOL, SOL_remain_col_lists);
    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOL, 10);
    // std::cerr << "\n";

    start_time = clock();
    SemiJoin(partyIdx, srvs, Scustomer, 0, -1, 2, SOL, 0, -1, 2, 1, AggFunc::SUM, SCOL);
    // c_custkey, c_nationkey, c_custkey_pi, o_tag_sum
    end_time = clock();
    double COL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "LOC phase time cost = " << LOC_time << std::endl;
    std::cout << "total time cost = " << OL_time + COL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;

    // std::cerr << "\nshared LOC \n";
    STableReveal(srvs, SCOL, 10);
    // std::cerr << "\n";
}

void Q10Baseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and c_nationkey
    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ10/" + scale + "/customer_pi.csv", raw_customer);
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

    // o_custkey, o_orderkey, o_orderflag
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderflag", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ10/" + scale + "/orders_pi.csv", raw_orders);
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

    // l_orderkey, (l_extendedprice * (1 - l_discount)) AS l_revenue
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_revenue", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ10/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    SharedTable SCOL, SOL;
    auto start_time = clock();
    SemiJoinBaseline(partyIdx, srvs, Sorders, 1, -1, 4, Slineitem, 0, -1, 2, 1, AggFunc::SUM, SOL);
    // o_custkey, o_orderkey, o_orderflag, o_custkey_pi, o_orderkey_pi, l_sum
    ColBitAnd(partyIdx, srvs, SOL.mColumns[2], SOL.mColumns[5]);
    std::vector<u64> SOL_remain_col_lists = {0, 5, 3};
    // o_custkey, o_tag_sum, o_custkey_pi
    TakeColumns(SOL, SOL_remain_col_lists);
    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "OC phase time cost = " << OC_time << std::endl;
    // std::cerr << "\nshared OC \n";
    // STableReveal(srvs, SOL, 10);
    // std::cerr << "\n";

    start_time = clock();
    SemiJoinBaseline(partyIdx, srvs, Scustomer, 0, -1, 2, SOL, 0, -1, 2, 1, AggFunc::SUM, SCOL);
    // c_custkey, c_nationkey, c_custkey_pi, o_tag_sum
    end_time = clock();
    double COL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    // std::cout << "LOC phase time cost = " << LOC_time << std::endl;
    std::cout << "total time cost = " << OL_time + COL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    std::cout << "IN+OUT = " << Scustomer.rows() * 2 * 8 + Slineitem.rows() * 2 * 8 + Sorders.rows() * 3 * 8 + SCOL.rows() * 3 * 8 << std::endl;
    // std::cerr << "\nshared LOC \n";
    // STableReveal(srvs, SCOL, 10);
    // std::cerr << "\n";
}


int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    // Q10(partyIdx, scale);
    Q10Baseline(partyIdx, scale);
    return 0;
}