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

void GenRanksCustomer(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_original_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCHQ10/" + scale + "/customer_pi.csv", raw_customer);
    u64 customer_nrows = raw_customer.size();
    Table customer(customer_nrows, customerColInfo);
    for (auto i=0; i<customer_nrows; ++i) {
        customer.mColumns[0].mData(i, 0) = raw_customer[i][0];
        customer.mColumns[1].mData(i, 0) = i+1;
        customer.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Scustomer;
    if (partyIdx == 0) {
        Scustomer = srvs.localInput(customer);
    } else {
        Scustomer = srvs.remoteInput(0);
    }
    
    auto start_time = clock();

    ShuffleQuickSort(partyIdx, srvs, Scustomer, 0);
    for (auto i=0; i<Scustomer.rows(); ++i) {
        Scustomer.mColumns[2].mShares[0](i) = Scustomer.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;

    perm.SSPerm(partyIdx, srvs, Scustomer, 1);
    auto end_time = clock();
    
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void GenRanksOrders(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and c_nationkey
    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_original_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ10/" + scale + "/orders_pi.csv", raw_orders);
    u64 nrows = raw_orders.size();
    Table orders(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        orders.mColumns[0].mData(i, 0) = raw_orders[i][0];
        orders.mColumns[1].mData(i, 0) = i+1;
        orders.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Sorders;
    if (partyIdx == 0) {
        Sorders = srvs.localInput(orders);
    } else {
        Sorders = srvs.remoteInput(0);
    }
    
    auto start_time = clock();

    ShuffleQuickSort(partyIdx, srvs, Sorders, 0);
    for (auto i=0; i<Sorders.rows(); ++i) {
        Sorders.mColumns[2].mShares[0](i) = Sorders.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;

    perm.SSPerm(partyIdx, srvs, Sorders, 1);
    auto end_time = clock();
    
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}


void GenRanksLineitem(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // c_custkey and c_nationkey
    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_original_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ10/" + scale + "/lineitem_pi.csv", raw_lineitem);
    u64 nrows = raw_lineitem.size();
    Table lineitem(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        lineitem.mColumns[0].mData(i, 0) = raw_lineitem[i][0];
        lineitem.mColumns[1].mData(i, 0) = i+1;
        lineitem.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Slineitem;
    if (partyIdx == 0) {
        Slineitem = srvs.localInput(lineitem);
    } else {
        Slineitem = srvs.remoteInput(0);
    }
    
    auto start_time = clock();

    ShuffleQuickSort(partyIdx, srvs, Slineitem, 0);
    for (auto i=0; i<Slineitem.rows(); ++i) {
        Slineitem.mColumns[2].mShares[0](i) = Slineitem.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;

    perm.SSPerm(partyIdx, srvs, Slineitem, 1);
    auto end_time = clock();
    
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void GenRanksNation(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "original_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nrows = raw_nation.size();
    Table nation(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        nation.mColumns[0].mData(i, 0) = raw_nation[i][0];
        nation.mColumns[1].mData(i, 0) = i+1;
        nation.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }
    
    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Snation, 0);
    for (auto i=0; i<Snation.rows(); ++i) {
        Snation.mColumns[2].mShares[0](i) = Snation.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;
    perm.SSPerm(partyIdx, srvs, Snation, 1);
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void GenRanksRegion(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "original_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_nation);
    u64 nrows = raw_nation.size();
    Table nation(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        nation.mColumns[0].mData(i, 0) = raw_nation[i][0];
        nation.mColumns[1].mData(i, 0) = i+1;
        nation.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }
    
    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Snation, 0);
    for (auto i=0; i<Snation.rows(); ++i) {
        Snation.mColumns[2].mShares[0](i) = Snation.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;
    perm.SSPerm(partyIdx, srvs, Snation, 1);
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void GenRanksSupplier(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "original_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_nation);
    u64 nrows = raw_nation.size();
    Table nation(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        nation.mColumns[0].mData(i, 0) = raw_nation[i][0];
        nation.mColumns[1].mData(i, 0) = i+1;
        nation.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }
    
    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Snation, 0);
    for (auto i=0; i<Snation.rows(); ++i) {
        Snation.mColumns[2].mShares[0](i) = Snation.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;
    perm.SSPerm(partyIdx, srvs, Snation, 1);
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void GenRanksPartsupp(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "original_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCHQ11/" + scale + "/partsupp_pi.csv", raw_nation);
    u64 nrows = raw_nation.size();
    Table nation(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        nation.mColumns[0].mData(i, 0) = raw_nation[i][0];
        nation.mColumns[1].mData(i, 0) = i+1;
        nation.mColumns[2].mData(i, 0) = 0;
        
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }
    
    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Snation, 0);
    for (auto i=0; i<Snation.rows(); ++i) {
        Snation.mColumns[2].mShares[0](i) = Snation.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;
    perm.SSPerm(partyIdx, srvs, Snation, 1);
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

void SimSortCost(u64 partyIdx, u64 nrows) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "a", TypeID::IntID, 64 }, ColumnInfo{ "b", TypeID::IntID, 64 }, ColumnInfo{ "c", TypeID::IntID, 64 }, ColumnInfo{ "d", TypeID::IntID, 64 }, ColumnInfo{ "e", TypeID::IntID, 64 } };
    Table nation(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        nation.mColumns[0].mData(i, 0) = i+1;
        nation.mColumns[1].mData(i, 0) = i+1;
        nation.mColumns[2].mData(i, 0) = i+1;
        nation.mColumns[3].mData(i, 0) = i+1;
        nation.mColumns[4].mData(i, 0) = i+1;
        
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }
    
    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Snation, 0);
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    GenRanksLineitem(partyIdx, scale);
    GenRanksOrders(partyIdx, scale);
    GenRanksCustomer(partyIdx, scale);
    GenRanksNation(partyIdx, scale);
    GenRanksRegion(partyIdx, scale);
    GenRanksSupplier(partyIdx, scale);
    GenRanksPartsupp(partyIdx, scale);

    // u64 nout = 0, boundk = scale[0] - '0';
    // if (boundk == 2) {
    //     nout = 3817909;
    // } else if (boundk == 3) {
    //     nout = 887494;
    // } else if (boundk == 4) {
    //     nout = 234827;
    // } else if (boundk == 5) {
    //     nout = 94920;
    // } else if (boundk == 6) {
    //     nout = 21151;
    // }
    // SimSortCost(partyIdx, nout);
    return 0;
}