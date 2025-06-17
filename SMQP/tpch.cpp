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

void FullJoin1(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCH/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCH/" + scale + "/orders_pi.csv", raw_orders);
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

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCH/" + scale + "/customer_pi.csv", raw_customer);
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

    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nation_nrows = raw_nation.size();
    Table nation(nation_nrows, nationColInfo);
    for (auto i=0; i<nation_nrows; ++i) {
        for (auto j=0; j<raw_nation[i].size(); ++j) {
            nation.mColumns[j].mData(i, 0) = raw_nation[i][j];
        }
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> regionColInfo = { ColumnInfo{ "r_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "r_regionkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_region;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_region);
    u64 region_nrows = raw_region.size();
    Table region(region_nrows, regionColInfo);
    for (auto i=0; i<region_nrows; ++i) {
        for (auto j=0; j<raw_region[i].size(); ++j) {
            region.mColumns[j].mData(i, 0) = raw_region[i][j];
        }
    }
    SharedTable Sregion;
    if (partyIdx == 0) {
        Sregion = srvs.localInput(region);
    } else {
        Sregion = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_supplier);
    u64 supplier_nrows = raw_supplier.size();
    Table supplier(supplier_nrows, supplierColInfo);
    for (auto i=0; i<supplier_nrows; ++i) {
        for (auto j=0; j<raw_supplier[i].size(); ++j) {
            supplier.mColumns[j].mData(i, 0) = raw_supplier[i][j];
        }
    }
    SharedTable Ssupplier;
    if (partyIdx == 0) {
        Ssupplier = srvs.localInput(supplier);
    } else {
        Ssupplier = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> partColInfo = { ColumnInfo{ "p_partkey", TypeID::IntID, 64 }, ColumnInfo{ "p_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_part;
    LoadData("../SMQP/TPCH/" + scale + "/part_pi.csv", raw_part);
    u64 part_nrows = raw_part.size();
    Table part(part_nrows, partColInfo);
    for (auto i=0; i<part_nrows; ++i) {
        for (auto j=0; j<raw_part[i].size(); ++j) {
            part.mColumns[j].mData(i, 0) = raw_part[i][j];
        }
    }
    SharedTable Spart;
    if (partyIdx == 0) {
        Spart = srvs.localInput(part);
    } else {
        Spart = srvs.remoteInput(0);
    }

    SharedTable NR, CNR, OCNR, LS, LSP, LSPOCNR;

    auto start_time = clock();

    // nation join region
    PKJoin(partyIdx, srvs, Snation, 1, -1, 3, Sregion, 0, -1, 1, {}, NR);
    std::vector<u64> remain_col_list = {0, 1, 2};
    TakeColumns(NR, remain_col_list); // NR: nationkey, regionkey, nationkey_pi

    // STableReveal(srvs, NR, 10);

    // customer join NR
    PKJoin(partyIdx, srvs, Scustomer, 1, -1, 3, NR, 0, -1, 2, {}, CNR);
    remain_col_list = {0, 1, 5, 2};
    TakeColumns(CNR, remain_col_list); // CNR: custkey, nationkey, regionkey, custkey_pi

    // STableReveal(srvs, CNR, 10);

    // orders join CNR
    PKJoin(partyIdx, srvs, Sorders, 1, -1, 3, CNR, 0, -1, 3, {}, OCNR);
    remain_col_list = {0, 1, 5, 6, 2};
    TakeColumns(OCNR, remain_col_list); // OCNR: orderkey, custkey, nationkey, regionkey, orderkey_pi

    // STableReveal(srvs, OCNR, 10);

    PKJoin(partyIdx, srvs, Slineitem, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 7, 8, 9};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey

    /*
    // lineitem join supplier
    PKJoin(partyIdx, srvs, Slineitem, 1, -1, 4, Ssupplier, 0, -1, 1, {}, LS);
    remain_col_list = {0, 1, 2, 3, 5};
    TakeColumns(LS, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi, partkey_pi

    // STableReveal(srvs, LS, 10);

    // LS join part
    PKJoin(partyIdx, srvs, LS, 2, -1, 4, Spart, 0, -1, 1, {}, LSP);
    remain_col_list = {0, 1, 2, 3};
    TakeColumns(LSP, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi

    // STableReveal(srvs, LSP, 10);

    // LSP join OCNR
    PKJoin(partyIdx, srvs, LSP, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 5, 6, 7};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey

    // STableReveal(srvs, LSPOCNR, 10);
    */

    auto end_time = clock();
    double time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "total time cost = " << time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 2 * 8 + Snation.rows() * 2 * 8 +  Sregion.rows() * 8 + Ssupplier.rows() * 8 + Spart.rows() * 8 + LSPOCNR.rows() * 6 * 8 << std::endl;
}

void FullJoin(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCH/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCH/" + scale + "/orders_pi.csv", raw_orders);
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

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCH/" + scale + "/customer_pi.csv", raw_customer);
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

    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nation_nrows = raw_nation.size();
    Table nation(nation_nrows, nationColInfo);
    for (auto i=0; i<nation_nrows; ++i) {
        for (auto j=0; j<raw_nation[i].size(); ++j) {
            nation.mColumns[j].mData(i, 0) = raw_nation[i][j];
        }
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> regionColInfo = { ColumnInfo{ "r_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "r_regionkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_region;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_region);
    u64 region_nrows = raw_region.size();
    Table region(region_nrows, regionColInfo);
    for (auto i=0; i<region_nrows; ++i) {
        for (auto j=0; j<raw_region[i].size(); ++j) {
            region.mColumns[j].mData(i, 0) = raw_region[i][j];
        }
    }
    SharedTable Sregion;
    if (partyIdx == 0) {
        Sregion = srvs.localInput(region);
    } else {
        Sregion = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_supplier);
    u64 supplier_nrows = raw_supplier.size();
    Table supplier(supplier_nrows, supplierColInfo);
    for (auto i=0; i<supplier_nrows; ++i) {
        for (auto j=0; j<raw_supplier[i].size(); ++j) {
            supplier.mColumns[j].mData(i, 0) = raw_supplier[i][j];
        }
    }
    SharedTable Ssupplier;
    if (partyIdx == 0) {
        Ssupplier = srvs.localInput(supplier);
    } else {
        Ssupplier = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> partColInfo = { ColumnInfo{ "p_partkey", TypeID::IntID, 64 }, ColumnInfo{ "p_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_part;
    LoadData("../SMQP/TPCH/" + scale + "/part_pi.csv", raw_part);
    u64 part_nrows = raw_part.size();
    Table part(part_nrows, partColInfo);
    for (auto i=0; i<part_nrows; ++i) {
        for (auto j=0; j<raw_part[i].size(); ++j) {
            part.mColumns[j].mData(i, 0) = raw_part[i][j];
        }
    }
    SharedTable Spart;
    if (partyIdx == 0) {
        Spart = srvs.localInput(part);
    } else {
        Spart = srvs.remoteInput(0);
    }

    SharedTable NR, CNR, OCNR, LS, LSP, LSPOCNR;

    auto start_time = clock();

    // nation join region
    PKJoin(partyIdx, srvs, Snation, 1, -1, 3, Sregion, 0, -1, 1, {}, NR);
    std::vector<u64> remain_col_list = {0, 1, 2};
    TakeColumns(NR, remain_col_list); // NR: nationkey, regionkey, nationkey_pi

    // STableReveal(srvs, NR, 10);

    // customer join NR
    PKJoin(partyIdx, srvs, Scustomer, 1, -1, 3, NR, 0, -1, 2, {}, CNR);
    remain_col_list = {0, 1, 5, 2};
    TakeColumns(CNR, remain_col_list); // CNR: custkey, nationkey, regionkey, custkey_pi

    // STableReveal(srvs, CNR, 10);

    // orders join CNR
    PKJoin(partyIdx, srvs, Sorders, 1, -1, 3, CNR, 0, -1, 3, {}, OCNR);
    remain_col_list = {0, 1, 5, 6, 2};
    TakeColumns(OCNR, remain_col_list); // OCNR: orderkey, custkey, nationkey, regionkey, orderkey_pi

    // STableReveal(srvs, OCNR, 10);

    // lineitem join supplier
    PKJoin(partyIdx, srvs, Slineitem, 1, -1, 4, Ssupplier, 0, -1, 1, {}, LS);
    remain_col_list = {0, 1, 2, 3};
    TakeColumns(LS, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi

    // STableReveal(srvs, LS, 10);

    PKJoin(partyIdx, srvs, LS, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 5, 6, 7};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey



    auto end_time = clock();
    double time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "total time cost = " << time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 2 * 8 + Snation.rows() * 2 * 8 +  Sregion.rows() * 8 + Ssupplier.rows() * 8 + Spart.rows() * 8 + LSPOCNR.rows() * 6 * 8 << std::endl;
}

void FullJoin2(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCH/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCH/" + scale + "/orders_pi.csv", raw_orders);
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

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCH/" + scale + "/customer_pi.csv", raw_customer);
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

    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nation_nrows = raw_nation.size();
    Table nation(nation_nrows, nationColInfo);
    for (auto i=0; i<nation_nrows; ++i) {
        for (auto j=0; j<raw_nation[i].size(); ++j) {
            nation.mColumns[j].mData(i, 0) = raw_nation[i][j];
        }
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> regionColInfo = { ColumnInfo{ "r_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "r_regionkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_region;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_region);
    u64 region_nrows = raw_region.size();
    Table region(region_nrows, regionColInfo);
    for (auto i=0; i<region_nrows; ++i) {
        for (auto j=0; j<raw_region[i].size(); ++j) {
            region.mColumns[j].mData(i, 0) = raw_region[i][j];
        }
    }
    SharedTable Sregion;
    if (partyIdx == 0) {
        Sregion = srvs.localInput(region);
    } else {
        Sregion = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_supplier);
    u64 supplier_nrows = raw_supplier.size();
    Table supplier(supplier_nrows, supplierColInfo);
    for (auto i=0; i<supplier_nrows; ++i) {
        for (auto j=0; j<raw_supplier[i].size(); ++j) {
            supplier.mColumns[j].mData(i, 0) = raw_supplier[i][j];
        }
    }
    SharedTable Ssupplier;
    if (partyIdx == 0) {
        Ssupplier = srvs.localInput(supplier);
    } else {
        Ssupplier = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> partColInfo = { ColumnInfo{ "p_partkey", TypeID::IntID, 64 }, ColumnInfo{ "p_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_part;
    LoadData("../SMQP/TPCH/" + scale + "/part_pi.csv", raw_part);
    u64 part_nrows = raw_part.size();
    Table part(part_nrows, partColInfo);
    for (auto i=0; i<part_nrows; ++i) {
        for (auto j=0; j<raw_part[i].size(); ++j) {
            part.mColumns[j].mData(i, 0) = raw_part[i][j];
        }
    }
    SharedTable Spart;
    if (partyIdx == 0) {
        Spart = srvs.localInput(part);
    } else {
        Spart = srvs.remoteInput(0);
    }

    SharedTable NR, CNR, OCNR, LS, LSP, LSPOCNR;

    auto start_time = clock();

    PKJoin(partyIdx, srvs, Slineitem, 1, -1, 4, Ssupplier, 0, -1, 1, {}, LS);
    std::vector<u64> remain_col_list = {0, 1, 2, 3, 5};
    TakeColumns(LS, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi, partkey_pi

    // STableReveal(srvs, LS, 10);

    // LS join part
    PKJoin(partyIdx, srvs, LS, 2, -1, 4, Spart, 0, -1, 1, {}, LSP);
    remain_col_list = {0, 1, 2, 3};
    TakeColumns(LSP, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi

    // STableReveal(srvs, LSP, 10);

    auto end_time = clock();
    double time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "total time cost = " << time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 2 * 8 + Snation.rows() * 2 * 8 +  Sregion.rows() * 8 + Ssupplier.rows() * 8 + Spart.rows() * 8 + LSPOCNR.rows() * 6 * 8 << std::endl;
}


void FullJoin1Baseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCH/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCH/" + scale + "/orders_pi.csv", raw_orders);
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

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCH/" + scale + "/customer_pi.csv", raw_customer);
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

    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nation_nrows = raw_nation.size();
    Table nation(nation_nrows, nationColInfo);
    for (auto i=0; i<nation_nrows; ++i) {
        for (auto j=0; j<raw_nation[i].size(); ++j) {
            nation.mColumns[j].mData(i, 0) = raw_nation[i][j];
        }
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> regionColInfo = { ColumnInfo{ "r_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "r_regionkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_region;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_region);
    u64 region_nrows = raw_region.size();
    Table region(region_nrows, regionColInfo);
    for (auto i=0; i<region_nrows; ++i) {
        for (auto j=0; j<raw_region[i].size(); ++j) {
            region.mColumns[j].mData(i, 0) = raw_region[i][j];
        }
    }
    SharedTable Sregion;
    if (partyIdx == 0) {
        Sregion = srvs.localInput(region);
    } else {
        Sregion = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_supplier);
    u64 supplier_nrows = raw_supplier.size();
    Table supplier(supplier_nrows, supplierColInfo);
    for (auto i=0; i<supplier_nrows; ++i) {
        for (auto j=0; j<raw_supplier[i].size(); ++j) {
            supplier.mColumns[j].mData(i, 0) = raw_supplier[i][j];
        }
    }
    SharedTable Ssupplier;
    if (partyIdx == 0) {
        Ssupplier = srvs.localInput(supplier);
    } else {
        Ssupplier = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> partColInfo = { ColumnInfo{ "p_partkey", TypeID::IntID, 64 }, ColumnInfo{ "p_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_part;
    LoadData("../SMQP/TPCH/" + scale + "/part_pi.csv", raw_part);
    u64 part_nrows = raw_part.size();
    Table part(part_nrows, partColInfo);
    for (auto i=0; i<part_nrows; ++i) {
        for (auto j=0; j<raw_part[i].size(); ++j) {
            part.mColumns[j].mData(i, 0) = raw_part[i][j];
        }
    }
    SharedTable Spart;
    if (partyIdx == 0) {
        Spart = srvs.localInput(part);
    } else {
        Spart = srvs.remoteInput(0);
    }

    SharedTable NR, CNR, OCNR, LS, LSP, LSPOCNR;

    auto start_time = clock();

    // nation join region
    PKJoinBaseline(partyIdx, srvs, Snation, 1, -1, 3, Sregion, 0, -1, 1, {}, NR);
    std::vector<u64> remain_col_list = {0, 1, 2};
    TakeColumns(NR, remain_col_list); // NR: nationkey, regionkey, nationkey_pi

    // STableReveal(srvs, NR, 10);

    // customer join NR
    PKJoinBaseline(partyIdx, srvs, Scustomer, 1, -1, 3, NR, 0, -1, 2, {}, CNR);
    remain_col_list = {0, 1, 5, 2};
    TakeColumns(CNR, remain_col_list); // CNR: custkey, nationkey, regionkey, custkey_pi

    // STableReveal(srvs, CNR, 10);

    // orders join CNR
    PKJoinBaseline(partyIdx, srvs, Sorders, 1, -1, 3, CNR, 0, -1, 3, {}, OCNR);
    remain_col_list = {0, 1, 5, 6, 2};
    TakeColumns(OCNR, remain_col_list); // OCNR: orderkey, custkey, nationkey, regionkey, orderkey_pi

    // STableReveal(srvs, OCNR, 10);


    PKJoinBaseline(partyIdx, srvs, Slineitem, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 7, 8, 9};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey

    /*

    // lineitem join supplier
    PKJoinBaseline(partyIdx, srvs, Slineitem, 1, -1, 4, Ssupplier, 0, -1, 1, {}, LS);
    remain_col_list = {0, 1, 2, 3, 5};
    TakeColumns(LS, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi, partkey_pi

    // STableReveal(srvs, LS, 10);

    // LS join part
    PKJoinBaseline(partyIdx, srvs, LS, 2, -1, 4, Spart, 0, -1, 1, {}, LSP);
    remain_col_list = {0, 1, 2, 3};
    TakeColumns(LSP, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi

    // STableReveal(srvs, LSP, 10);

    // LSP join OCNR
    PKJoinBaseline(partyIdx, srvs, LSP, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 5, 6, 7};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey

    // STableReveal(srvs, LSPOCNR, 10);

    */

    auto end_time = clock();
    double time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "total time cost = " << time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 2 * 8 + Snation.rows() * 2 * 8 +  Sregion.rows() * 8 + Ssupplier.rows() * 8 + Spart.rows() * 8 + LSPOCNR.rows() * 6 * 8 << std::endl;
}


void FullJoinBaseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "l_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCH/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_custkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCH/" + scale + "/orders_pi.csv", raw_orders);
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

    std::vector<ColumnInfo> customerColInfo = { ColumnInfo{ "c_custkey", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "c_custkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "c_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_customer;
    LoadData("../SMQP/TPCH/" + scale + "/customer_pi.csv", raw_customer);
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

    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "n_regionkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCH/" + scale + "/nation_pi.csv", raw_nation);
    u64 nation_nrows = raw_nation.size();
    Table nation(nation_nrows, nationColInfo);
    for (auto i=0; i<nation_nrows; ++i) {
        for (auto j=0; j<raw_nation[i].size(); ++j) {
            nation.mColumns[j].mData(i, 0) = raw_nation[i][j];
        }
    }
    SharedTable Snation;
    if (partyIdx == 0) {
        Snation = srvs.localInput(nation);
    } else {
        Snation = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> regionColInfo = { ColumnInfo{ "r_regionkey", TypeID::IntID, 64 }, ColumnInfo{ "r_regionkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_region;
    LoadData("../SMQP/TPCH/" + scale + "/region_pi.csv", raw_region);
    u64 region_nrows = raw_region.size();
    Table region(region_nrows, regionColInfo);
    for (auto i=0; i<region_nrows; ++i) {
        for (auto j=0; j<raw_region[i].size(); ++j) {
            region.mColumns[j].mData(i, 0) = raw_region[i][j];
        }
    }
    SharedTable Sregion;
    if (partyIdx == 0) {
        Sregion = srvs.localInput(region);
    } else {
        Sregion = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCH/" + scale + "/supplier_pi.csv", raw_supplier);
    u64 supplier_nrows = raw_supplier.size();
    Table supplier(supplier_nrows, supplierColInfo);
    for (auto i=0; i<supplier_nrows; ++i) {
        for (auto j=0; j<raw_supplier[i].size(); ++j) {
            supplier.mColumns[j].mData(i, 0) = raw_supplier[i][j];
        }
    }
    SharedTable Ssupplier;
    if (partyIdx == 0) {
        Ssupplier = srvs.localInput(supplier);
    } else {
        Ssupplier = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> partColInfo = { ColumnInfo{ "p_partkey", TypeID::IntID, 64 }, ColumnInfo{ "p_partkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_part;
    LoadData("../SMQP/TPCH/" + scale + "/part_pi.csv", raw_part);
    u64 part_nrows = raw_part.size();
    Table part(part_nrows, partColInfo);
    for (auto i=0; i<part_nrows; ++i) {
        for (auto j=0; j<raw_part[i].size(); ++j) {
            part.mColumns[j].mData(i, 0) = raw_part[i][j];
        }
    }
    SharedTable Spart;
    if (partyIdx == 0) {
        Spart = srvs.localInput(part);
    } else {
        Spart = srvs.remoteInput(0);
    }

    SharedTable NR, CNR, OCNR, LS, LSP, LSPOCNR;

    auto start_time = clock();

    // nation join region
    PKJoinBaseline(partyIdx, srvs, Snation, 1, -1, 3, Sregion, 0, -1, 1, {}, NR);
    std::vector<u64> remain_col_list = {0, 1, 2};
    TakeColumns(NR, remain_col_list); // NR: nationkey, regionkey, nationkey_pi

    // STableReveal(srvs, NR, 10);

    // customer join NR
    PKJoinBaseline(partyIdx, srvs, Scustomer, 1, -1, 3, NR, 0, -1, 2, {}, CNR);
    remain_col_list = {0, 1, 5, 2};
    TakeColumns(CNR, remain_col_list); // CNR: custkey, nationkey, regionkey, custkey_pi

    // STableReveal(srvs, CNR, 10);

    // orders join CNR
    PKJoinBaseline(partyIdx, srvs, Sorders, 1, -1, 3, CNR, 0, -1, 3, {}, OCNR);
    remain_col_list = {0, 1, 5, 6, 2};
    TakeColumns(OCNR, remain_col_list); // OCNR: orderkey, custkey, nationkey, regionkey, orderkey_pi

    // STableReveal(srvs, OCNR, 10);

    // lineitem join supplier
    PKJoinBaseline(partyIdx, srvs, Slineitem, 1, -1, 4, Ssupplier, 0, -1, 1, {}, LS);
    remain_col_list = {0, 1, 2, 3};
    TakeColumns(LS, remain_col_list); // orderkey, suppkey, partkey, orderkey_pi

    // STableReveal(srvs, LS, 10);

    PKJoinBaseline(partyIdx, srvs, LS, 0, -1, 3, OCNR, 0, -1, 4, {}, LSPOCNR);
    remain_col_list = {0, 1, 2, 5, 6, 7};
    TakeColumns(LSPOCNR, remain_col_list); // orderkey, suppkey, partkey, custkey, nationkey, regionkey



    auto end_time = clock();
    double time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "total time cost = " << time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
    std::cout << "IN+OUT =  " << Slineitem.rows() * 3 * 8 + Scustomer.rows() * 2 * 8 + Sorders.rows() * 2 * 8 + Snation.rows() * 2 * 8 +  Sregion.rows() * 8 + Ssupplier.rows() * 8 + LSPOCNR.rows() * 6 * 8 << std::endl;
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    // FullJoin(partyIdx, scale);
    FullJoinBaseline(partyIdx, scale);
    return 0;
}