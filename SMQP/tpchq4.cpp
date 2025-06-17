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

void STableRevealToFIle(DBServer& srvs, SharedTable tab, std::string filepath) {
    freopen(filepath.c_str(), "w", stdout);
    u64 nrows = tab.rows(), nattrs = tab.mColumns.size();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    for (auto i=0; i<nattrs; ++i) {
        RCols[i].resize(nrows, 1);
        srvs.mEnc.revealAll(srvs.mRt.mComm, tab.mColumns[i], RCols[i]);
    }
    
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<nattrs; ++j) {
            std::cout << RCols[j](i) << ' ';
        }
        std::cout << std::endl;
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

void Q4(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // o_orderkey, o_orderpriority, (o_orderdate >= '1993-01-07' AND o_orderdate < '1993-04-07') AS o_tag, o_orderkey_pi, o_orderpriority_pi
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderpriority", TypeID::IntID, 64 }, ColumnInfo{ "o_tag", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderpriority_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ4/" + scale + "/orders_pi.csv", raw_orders);
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

    // l_orderkey, (l_commitdate < l_receiptdate) AS l_tag, l_orderkey_pi
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ4/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    SharedTable SOL;
    auto start_time = clock();

    SemiJoin(partyIdx, srvs, Sorders, 0, -1, 3, Slineitem, 0, -1, 2, 1, AggFunc::EXISTS, SOL);
    ColBitAnd(partyIdx, srvs, SOL.mColumns[5], SOL.mColumns[2]);

    std::vector<u64> SOL_remain_col_lists = {1, 2, 4};
    TakeColumns(SOL, SOL_remain_col_lists);

    Permutation perm;
    perm.SSPerm(partyIdx, srvs, SOL, 2, 1);
    SegmentedPrefixSum(partyIdx, srvs, SOL.mColumns[0], SOL.mColumns[1]);

    u64 nrows = SOL.rows();
    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = SOL.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = SOL.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = SOL.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = SOL.mColumns[0].mShares[1](i+1);
    }
    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, SOL.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);

    for (auto i=0; i<SOL.rows() - 1; ++i) {
        SOL.mColumns[2].mShares[0](i) = eqtag.mShares[0](i);
        SOL.mColumns[2].mShares[1](i) = eqtag.mShares[1](i);
    }
    SOL.mColumns[2].mShares[0](SOL.rows() - 1) = SOL.mColumns[2].mShares[1](SOL.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, SOL, 2);

    SOL_remain_col_lists = {0, 1};
    TakeColumns(SOL, SOL_remain_col_lists);
    // STableReveal(srvs, SOL);

    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "total time cost = " << OL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    std::cout << "IN+OUT = " << Slineitem.rows() * 2 * 8 + Sorders.rows() * 3 * 8 + SOL.rows() * 2 * 8 << std::endl;
}

void Q4Baseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // o_orderkey, o_orderpriority, (o_orderdate >= '1993-01-07' AND o_orderdate < '1993-04-07') AS o_tag, o_orderkey_pi, o_orderpriority_pi
    std::vector<ColumnInfo> ordersColInfo = { ColumnInfo{ "o_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "o_orderpriority", TypeID::IntID, 64 }, ColumnInfo{ "o_tag", TypeID::IntID, 64 }, ColumnInfo{ "o_orderkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "o_orderpriority_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_orders;
    LoadData("../SMQP/TPCHQ4/" + scale + "/orders_pi.csv", raw_orders);
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

    // l_orderkey, (l_commitdate < l_receiptdate) AS l_tag, l_orderkey_pi
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_orderkey", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_orderkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ4/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    SharedTable SOL;
    auto start_time = clock();

    SemiJoinBaseline(partyIdx, srvs, Sorders, 0, -1, 3, Slineitem, 0, -1, 2, 1, AggFunc::EXISTS, SOL);
    ColBitAnd(partyIdx, srvs, SOL.mColumns[5], SOL.mColumns[2]);

    std::vector<u64> SOL_remain_col_lists = {1, 2, 4};
    TakeColumns(SOL, SOL_remain_col_lists);

    ShuffleQuickSort(partyIdx, srvs, SOL, 2, 1);
    SegmentedPrefixSum(partyIdx, srvs, SOL.mColumns[0], SOL.mColumns[1]);

    u64 nrows = SOL.rows();
    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = SOL.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = SOL.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = SOL.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = SOL.mColumns[0].mShares[1](i+1);
    }
    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, SOL.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);

    for (auto i=0; i<SOL.rows() - 1; ++i) {
        SOL.mColumns[2].mShares[0](i) = eqtag.mShares[0](i);
        SOL.mColumns[2].mShares[1](i) = eqtag.mShares[1](i);
    }
    SOL.mColumns[2].mShares[0](SOL.rows() - 1) = SOL.mColumns[2].mShares[1](SOL.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, SOL, 2);

    SOL_remain_col_lists = {0, 1};
    TakeColumns(SOL, SOL_remain_col_lists);
    // STableReveal(srvs, SOL);

    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << std::endl << "baseline " << std::endl;
    std::cout << "total time cost = " << OL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    std::cout << "IN+OUT = " << Slineitem.rows() * 2 * 8 + Sorders.rows() * 3 * 8 + SOL.rows() * 2 * 8 << std::endl;
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    Q4(partyIdx, scale);
    // Q4Baseline(partyIdx, scale);
    return 0;
}