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

void Q11(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // ps_partkey, ps_suppkey, convert(ps_supplycost * ps_availqty * 100, SIGNED) as ps_value
    std::vector<ColumnInfo> partsuppColInfo = { ColumnInfo{ "ps_partkey", TypeID::IntID, 64 }, ColumnInfo{ "ps_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "ps_value", TypeID::IntID, 64 }, ColumnInfo{ "ps_partkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "ps_suppkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_partsupp;
    LoadData("../SMQP/TPCHQ11/" + scale + "/partsupp_pi.csv", raw_partsupp);
    u64 partsupp_nrows = raw_partsupp.size();
    Table partsupp(partsupp_nrows, partsuppColInfo);
    for (auto i=0; i<partsupp_nrows; ++i) {
        for (auto j=0; j<raw_partsupp[i].size(); ++j) {
            partsupp.mColumns[j].mData(i, 0) = raw_partsupp[i][j];
        }
    }
    SharedTable Spartsupp;
    if (partyIdx == 0) {
        Spartsupp = srvs.localInput(partsupp);
    } else {
        Spartsupp = srvs.remoteInput(0);
    }

    // s_suppkey, s_nationkey
    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "s_nationkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCHQ11/" + scale + "/supplier_pi.csv", raw_supplier);
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

    // n_nationkey, (n_name = 'ARGENTINA') as n_tag
    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_tag", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCHQ11/" + scale + "/nation_pi.csv", raw_nation);
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

    SharedTable SSN, SPSN;

    auto start_time = clock();

    SemiJoin(partyIdx, srvs, Ssupplier, 1, -1, 3, Snation, 0, -1, 2, 1, AggFunc::EXISTS, SSN);
    std::vector<u64> SSN_remain_col_lists = {0, 2, 4};
    TakeColumns(SSN, SSN_remain_col_lists);
    // SSN suppkey, suppkey_pi, tag

    SemiJoin(partyIdx, srvs, Spartsupp, 1, -1, 4, SSN, 0, -1, 1, 2, AggFunc::EXISTS, SPSN);
    ColBitAnd(partyIdx, srvs, SPSN.mColumns[5], SPSN.mColumns[2]);

    std::vector<u64> SPSN_remain_col_lists = {0, 2, 3};
    TakeColumns(SPSN, SPSN_remain_col_lists);

    Permutation perm;
    perm.SSPerm(partyIdx, srvs, SPSN, 2, 1);
    SegmentedPrefixSum(partyIdx, srvs, SPSN.mColumns[0], SPSN.mColumns[1]);

    u64 nrows = SPSN.rows();
    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = SPSN.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = SPSN.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = SPSN.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = SPSN.mColumns[0].mShares[1](i+1);
    }
    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, SPSN.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);

    for (auto i=0; i<SPSN.rows() - 1; ++i) {
        SPSN.mColumns[2].mShares[0](i) = eqtag.mShares[0](i);
        SPSN.mColumns[2].mShares[1](i) = eqtag.mShares[1](i);
    }
    SPSN.mColumns[2].mShares[0](SPSN.rows() - 1) = SPSN.mColumns[2].mShares[1](SPSN.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, SPSN, 2);

    SPSN_remain_col_lists = {0, 1};
    TakeColumns(SPSN, SPSN_remain_col_lists);
    CompactionZeroEntity(partyIdx, srvs, SPSN, 1);


    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "total time cost = " << OL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    std::cout << "IN+OUT = " << Ssupplier.rows() * 2 * 8 + Spartsupp.rows() * 3 * 8 + Snation.rows() * 2 * 8 + SPSN.rows() * 2 * 8 << std::endl;
    
    // STableReveal(srvs, SPSN);
}


void Q11Baseline(u64 partyIdx, std::string scale) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // ps_partkey, ps_suppkey, convert(ps_supplycost * ps_availqty * 100, SIGNED) as ps_value
    std::vector<ColumnInfo> partsuppColInfo = { ColumnInfo{ "ps_partkey", TypeID::IntID, 64 }, ColumnInfo{ "ps_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "ps_value", TypeID::IntID, 64 }, ColumnInfo{ "ps_partkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "ps_suppkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_partsupp;
    LoadData("../SMQP/TPCHQ11/" + scale + "/partsupp_pi.csv", raw_partsupp);
    u64 partsupp_nrows = raw_partsupp.size();
    Table partsupp(partsupp_nrows, partsuppColInfo);
    for (auto i=0; i<partsupp_nrows; ++i) {
        for (auto j=0; j<raw_partsupp[i].size(); ++j) {
            partsupp.mColumns[j].mData(i, 0) = raw_partsupp[i][j];
        }
    }
    SharedTable Spartsupp;
    if (partyIdx == 0) {
        Spartsupp = srvs.localInput(partsupp);
    } else {
        Spartsupp = srvs.remoteInput(0);
    }

    // s_suppkey, s_nationkey
    std::vector<ColumnInfo> supplierColInfo = { ColumnInfo{ "s_suppkey", TypeID::IntID, 64 }, ColumnInfo{ "s_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "s_suppkey_pi", TypeID::IntID, 64 }, ColumnInfo{ "s_nationkey_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_supplier;
    LoadData("../SMQP/TPCHQ11/" + scale + "/supplier_pi.csv", raw_supplier);
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

    // n_nationkey, (n_name = 'ARGENTINA') as n_tag
    std::vector<ColumnInfo> nationColInfo = { ColumnInfo{ "n_nationkey", TypeID::IntID, 64 }, ColumnInfo{ "n_tag", TypeID::IntID, 64 }, ColumnInfo{ "n_nationkey_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_nation;
    LoadData("../SMQP/TPCHQ11/" + scale + "/nation_pi.csv", raw_nation);
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

    SharedTable SSN, SPSN;

    auto start_time = clock();

    SemiJoinBaseline(partyIdx, srvs, Ssupplier, 1, -1, 3, Snation, 0, -1, 2, 1, AggFunc::EXISTS, SSN);
    std::vector<u64> SSN_remain_col_lists = {0, 2, 4};
    TakeColumns(SSN, SSN_remain_col_lists);
    // SSN suppkey, suppkey_pi, tag

    SemiJoinBaseline(partyIdx, srvs, Spartsupp, 1, -1, 4, SSN, 0, -1, 1, 2, AggFunc::EXISTS, SPSN);
    ColBitAnd(partyIdx, srvs, SPSN.mColumns[5], SPSN.mColumns[2]);

    std::vector<u64> SPSN_remain_col_lists = {0, 2, 3};
    TakeColumns(SPSN, SPSN_remain_col_lists);

    ShuffleQuickSort(partyIdx, srvs, SPSN, 2, 0);
    SegmentedPrefixSum(partyIdx, srvs, SPSN.mColumns[0], SPSN.mColumns[1]);

    u64 nrows = SPSN.rows();
    sbMatrix eqin1(nrows - 1, 64), eqin2(nrows - 1, 64), eqtag(nrows - 1, 1);
    for (auto i=0; i<nrows - 1; ++i) {
        eqin1.mShares[0](i) = SPSN.mColumns[0].mShares[0](i);
        eqin1.mShares[1](i) = SPSN.mColumns[0].mShares[1](i);

        eqin2.mShares[0](i) = SPSN.mColumns[0].mShares[0](i+1);
        eqin2.mShares[1](i) = SPSN.mColumns[0].mShares[1](i+1);
    }
    BetaLibrary lib;
    auto circ = lib.int_neq(64);
    Sh3BinaryEvaluator eval;
    eval.setCir(circ, SPSN.rows() - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, eqin1);
    eval.setInput(1, eqin2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, eqtag);

    for (auto i=0; i<SPSN.rows() - 1; ++i) {
        SPSN.mColumns[2].mShares[0](i) = eqtag.mShares[0](i);
        SPSN.mColumns[2].mShares[1](i) = eqtag.mShares[1](i);
    }
    SPSN.mColumns[2].mShares[0](SPSN.rows() - 1) = SPSN.mColumns[2].mShares[1](SPSN.rows() - 1) = 1;

    CompactionZeroEntity(partyIdx, srvs, SPSN, 2);

    SPSN_remain_col_lists = {0, 1};
    TakeColumns(SPSN, SPSN_remain_col_lists);
    CompactionZeroEntity(partyIdx, srvs, SPSN, 1);


    auto end_time = clock();
    double OL_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cout << "total time cost = " << OL_time << std::endl;
    std::cout << "total comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()  << std::endl;
    std::cout << "IN+OUT = " << Ssupplier.rows() * 2 * 8 + Spartsupp.rows() * 3 * 8 + Snation.rows() * 2 * 8 + SPSN.rows() * 2 * 8 << std::endl;
    
    // STableReveal(srvs, SPSN);
}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    std::string scale = argv[2];

    Q11(partyIdx, scale);
    Q11Baseline(partyIdx, scale);
    return 0;
}