/*
select
    l_returnflag, l_linestatus, sum(l_quantity) as sum_qty
from
    lineitem
where
    l_shipdate <= '1998-09-02'
group by
    l_returnflag, l_linestatus
order by 
    l_returnflag, l_linestatus;
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

void STableReveal(DBServer& srvs, SharedTable tab, std::string filepath) {
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

void Q1(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // l_rfls (combined attribtue l_returnflag + l_linestatus), (l_shipdate <= '1998-09-02') as l_tag, l_quantity
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_rfls", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_quantity", TypeID::IntID, 64 }, ColumnInfo{ "l_rfls_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ1/" + scale + "/lineitem_pi.csv", raw_lineitem);
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

    auto start_time = clock();
    Permutation perm;
    // std::cerr << "total size" << sizeof(Slineitem) << std::endl;
    // std::cerr << "before start " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    perm.SSPerm(partyIdx, srvs, Slineitem, 3);
    // std::cerr << "SSPerm " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    ColBitAnd(partyIdx, srvs, Slineitem.mColumns[1], Slineitem.mColumns[2]);
    // std::cerr << "BitAnd " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    SegmentedPrefixSum(partyIdx, srvs, Slineitem.mColumns[0], Slineitem.mColumns[2]);
    // std::cerr << "PrefixSum " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    u64 nrows = Slineitem.rows();

    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = Slineitem.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = Slineitem.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = Slineitem.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = Slineitem.mColumns[0].mShares[1](i+1);
    }

    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, Slineitem.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);
    // std::cerr << "Check Equality " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    for (auto i=0; i<Slineitem.rows() - 1; ++i) {
        Slineitem.mColumns[1].mShares[0](i) = eqtag.mShares[0](i);
        Slineitem.mColumns[1].mShares[1](i) = eqtag.mShares[1](i);
    }
    Slineitem.mColumns[1].mShares[0](Slineitem.rows() - 1) = Slineitem.mColumns[1].mShares[1](Slineitem.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, Slineitem, 1);
    // std::cerr << "Compaction " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    STableReveal(srvs, Slineitem);

    auto end_time = clock();

    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent()  + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()<< std::endl;
    std::cout << "total running time = " << 1.0 * (end_time - start_time) / CLOCKS_PER_SEC << std::endl << std::endl;;
}

void Q1Baseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // l_rfls (combined attribtue l_returnflag + l_linestatus), (l_shipdate <= '1998-09-02') as l_tag, l_quantity
    std::vector<ColumnInfo> lineitemColInfo = { ColumnInfo{ "l_rfls", TypeID::IntID, 64 }, ColumnInfo{ "l_tag", TypeID::IntID, 64 }, ColumnInfo{ "l_quantity", TypeID::IntID, 64 }, ColumnInfo{ "l_rfls_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_lineitem;
    LoadData("../SMQP/TPCHQ1/" + scale + "/lineitem_pi.csv", raw_lineitem);
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
    Slineitem.mColumns.pop_back();

    auto start_time = clock();
    ShuffleQuickSort(partyIdx, srvs, Slineitem, 0, 0);
    ColBitAnd(partyIdx, srvs, Slineitem.mColumns[1], Slineitem.mColumns[2]);
    SegmentedPrefixSum(partyIdx, srvs, Slineitem.mColumns[0], Slineitem.mColumns[2]);

    u64 nrows = Slineitem.rows();

    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = Slineitem.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = Slineitem.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = Slineitem.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = Slineitem.mColumns[0].mShares[1](i+1);
    }

    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, Slineitem.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);

    for (auto i=0; i<Slineitem.rows() - 1; ++i) {
        Slineitem.mColumns[1].mShares[0](i) = eqtag.mShares[0](i);
        Slineitem.mColumns[1].mShares[1](i) = eqtag.mShares[1](i);
    }
    Slineitem.mColumns[1].mShares[0](Slineitem.rows() - 1) = Slineitem.mColumns[1].mShares[1](Slineitem.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, Slineitem, 1);
    STableReveal(srvs, Slineitem);

    auto end_time = clock();

    // std::cout << "IN + OUT = " << nrows * 3 * 8 + 4 * 2 * 8 << std::endl << std::endl;;

    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent()  + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()<< std::endl;
    std::cout << "total running time = " << 1.0 * (end_time - start_time) / CLOCKS_PER_SEC << std::endl << std::endl;
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    // Q1Baseline(partyIdx, scale);
    Q1(partyIdx, scale);
    return 0;
}