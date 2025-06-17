#include <iostream>
#include <vector>

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
    while (scanf("%d%c", &val, &ch) != EOF) {
        tuple.push_back(val);
        if (ch == '\n') {
            table.push_back(tuple);
            tuple.clear();
        }
    }
    return;
}


void L2F(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, bb;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    BinaryJoin(partyIdx, srvs, b1, 1, -1, 5, {}, b2, 0, -1, 4, {}, bb);
    // source, target, rating, time, source_pi, target_pi, source, target, rating, time, source_pi, target_pi

    std::vector<u64> bb_remain_col_lists = {0, 1, 7, 2, 3, 8, 9, 4};
    // source1, target1 = source2, target2, rating1, time1, rating2, time2
    TakeColumns(bb, bb_remain_col_lists);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "Binary Join phase time cost = " << bj_time << std::endl;
    std::cerr << "Binary Join phase comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;


    Permutation perm;
    perm.SSPerm(partyIdx, srvs, bb, 7);
    
    end_time = clock();
    double ob_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cerr << "\nOrder-by phase time cost = " << ob_time << std::endl;
    std::cerr << "Order-by phase comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;

    // std::cerr << "\nshared result \n";
    // STableReveal(srvs, bb, 10);
    // std::cerr << "\n";
    // std::cout << "Final Join Result = " << bb.rows() << std::endl;
}

void L2FBaseline(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, bb;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    BinaryJoinBaseline(partyIdx, srvs, b1, 1, -1, 5, {}, b2, 0, -1, 4, {}, bb);
    // source, target, rating, time, source_pi, target_pi, source, target, rating, time, source_pi, target_pi

    std::vector<u64> bb_remain_col_lists = {0, 1, 7, 2, 3, 8, 9, 4};
    // source1, target1 = source2, target2, rating1, time1, rating2, time2
    TakeColumns(bb, bb_remain_col_lists);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "Binary Join phase time cost = " << bj_time << std::endl;
    std::cerr << "Binary Join phase comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;


    ShuffleQuickSort(partyIdx, srvs, bb, 7);
    
    end_time = clock();
    double ob_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;
    std::cerr << "\nOrder-by phase time cost = " << ob_time << std::endl;
    std::cerr << "Order-by phase comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;

    // std::cerr << "\nshared result \n";
    // STableReveal(srvs, bb, 10);
    // std::cerr << "\n";
    // std::cout << "Final Join Result = " << bb.rows() << std::endl;
}

void L3C(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    SemiJoin(partyIdx, srvs, b2, 1, -1, 5, b3, 0, -1, 4, 2, AggFunc::SUM, b23);
    // b23: source, target, rating, time, source_pi, target_pi, sum_b3
    ColBitAnd(partyIdx, srvs, b23.mColumns[2], b23.mColumns[6]);

    SemiJoin(partyIdx, srvs, b1, 1, -1, 5, b23, 0, -1, 4, 6, AggFunc::SUM, b123);
    // b123: source, target, rating, time, source_pi, target_pi, sum_b3
    ColBitAnd(partyIdx, srvs, b123.mColumns[2], b123.mColumns[6]);
    PrefixSum(partyIdx, srvs, b123.mColumns[6]);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    // SBMatReveal(srvs, b123.mColumns[6]);

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}

void L3CBaseline(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    SemiJoinBaseline(partyIdx, srvs, b2, 1, -1, 5, b3, 0, -1, 4, 2, AggFunc::SUM, b23);
    // b23: source, target, rating, time, source_pi, target_pi, sum_b3
    ColBitAnd(partyIdx, srvs, b23.mColumns[2], b23.mColumns[6]);

    SemiJoinBaseline(partyIdx, srvs, b1, 1, -1, 5, b23, 0, -1, 4, 6, AggFunc::SUM, b123);
    // b123: source, target, rating, time, source_pi, target_pi, sum_b3
    ColBitAnd(partyIdx, srvs, b123.mColumns[2], b123.mColumns[6]);
    PrefixSum(partyIdx, srvs, b123.mColumns[6]);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    // SBMatReveal(srvs, b123.mColumns[6]);

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}

void L2FS(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();

    BinaryJoinWD(partyIdx, srvs, b2, 1, -1, 5, 2, {}, b3, 0, -1, 4, 2, {}, b23);
    
    // STableReveal(srvs, b23, 100);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    // SBMatReveal(srvs, b123.mColumns[6]);

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}

void L2FSBaseline(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }

    b1.mColumns.pop_back(); b1.mColumns.pop_back();
    b2.mColumns.pop_back(); b2.mColumns.pop_back();
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();

    BinaryJoinBaselineWD(partyIdx, srvs, b2, 1, -1, 2, b3, 0, -1, 2, b23);
    
    // STableReveal(srvs, b23, 100);

    auto end_time = clock();
    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    // SBMatReveal(srvs, b123.mColumns[6]);

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}


void L3FS(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    u64 nout = 887494;

    BinaryJoinWD(partyIdx, srvs, b2, 1, -1, 5, 2, {4}, b3, 0, -1, 4, 2, {4}, b23, nout);
    // STableReveal(srvs, b23, 10); std::cout << std::endl;

    std::vector<u64> col_remaining = {0, 1, 2, 3, 7, 8, 9, 4};
    TakeColumns(b23, col_remaining);
    // STableReveal(srvs, b23, 10); std::cout << std::endl;

    BinaryJoinWD(partyIdx, srvs, b1, 1, -1, 5, 2, {}, b23, 0, -1, 7, 2, {}, b123);

    // STableReveal(srvs, b123, 10);

    auto end_time = clock();

    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}

void L3FScut(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        raw_alpha[i].erase(raw_alpha[i].begin() + 3); // delete timestamp
    }
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, source_pi, target_pi
    auto start_time = clock();
    u64 nout = 887494;

    b3.mColumns.pop_back(); // b3 delete target_pi
    BinaryJoinWD(partyIdx, srvs, b2, 1, -1, 4, 2, {3}, b3, 0, -1, 3, 2, {3}, b23, nout);

    auto start_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    STableReveal(srvs, b23, 10); std::cout << std::endl;
    auto end_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    auto bias = (end_comm - start_comm);

    std::vector<u64> col_remaining = {0, 1, 2, 6, 7, 3};
    TakeColumns(b23, col_remaining);
    start_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    STableReveal(srvs, b23, 10); std::cout << std::endl;
    end_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    bias += (end_comm - start_comm);

    BinaryJoinWD(partyIdx, srvs, b1, 1, -1, 4, 2, {}, b23, 0, -1, 5, 2, {}, b123);

    start_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    STableReveal(srvs, b123, 10);
    end_comm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    bias += (end_comm - start_comm);

    auto end_time = clock();

    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() - bias << std::endl;
}

void L3FSBaseline(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, time, source_pi, target_pi
    auto start_time = clock();
    u64 nout = 887494;

    // b1.mColumns.pop_back(); b1.mColumns.pop_back();
    // b2.mColumns.pop_back(); b2.mColumns.pop_back();
    // b3.mColumns.pop_back(); b3.mColumns.pop_back();

    BinaryJoinBaselineWD(partyIdx, srvs, b2, 1, -1, 2, b3, 0, -1, 2, b23, nout);
    // STableReveal(srvs, b23, 10); std::cout << std::endl;

    std::vector<u64> col_remaining = {0, 1, 2, 3, 5, 6, 7};
    TakeColumns(b23, col_remaining);
    // STableReveal(srvs, b23, 10); std::cout << std::endl;

    BinaryJoinBaselineWD(partyIdx, srvs, b1, 1, -1, 2, b23, 0, -1, 2, b123);

    // STableReveal(srvs, b123, 10);

    auto end_time = clock();

    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}

void CartesianProduct(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "target", TypeID::IntID, 64 }, ColumnInfo{ "rating", TypeID::IntID, 64 }, ColumnInfo{ "time", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }, ColumnInfo{ "target_pi", TypeID::IntID, 64 } };
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        for (auto j=0; j<raw_alpha[i].size(); ++j) {
            alpha.mColumns[j].mData(i, 0) = raw_alpha[i][j];
        }
    }
    SharedTable b1, b2, b3, b23, b123;
    if (partyIdx == 0) {
        b1 = srvs.localInput(alpha);
        b2 = srvs.localInput(alpha);
        b3 = srvs.localInput(alpha);
    } else {
        b1 = srvs.remoteInput(0);
        b2 = srvs.remoteInput(0);
        b3 = srvs.remoteInput(0);
    }

    auto start_time = clock();
    // rating1 & rating2 & (target == source)
    // tag1 & tag2 & (a == b)

    // 24186 * 24186
    // 887494 * 24186
    // = 911680 * 24186

    // 10000 * 10000 : 
    // time cost = 56.5708
    // comm cost = 4781966872

    // 12000 * 12000 :
    // time cost = 82.2751
    // comm cost = 6882966872

    u64 firstn = 10000;
    sbMatrix tag1(firstn * firstn, 64);
    sbMatrix tag2(firstn * firstn, 64);
    sbMatrix a(firstn * firstn, 64), b(firstn * firstn, 64);
    u64 pos = 0;
    for (auto i=0; i<firstn; ++i) {
        for (auto j=0; j<firstn; ++j) {
            tag1.mShares[0](pos) = b1.mColumns[2].mShares[0](i);
            tag1.mShares[1](pos) = b1.mColumns[2].mShares[1](i);

            tag2.mShares[0](pos) = b2.mColumns[2].mShares[0](i);
            tag2.mShares[1](pos) = b2.mColumns[2].mShares[1](i);

            a.mShares[0](pos) = b1.mColumns[1].mShares[0](i);
            a.mShares[1](pos) = b1.mColumns[1].mShares[1](i);

            b.mShares[0](pos) = b2.mColumns[1].mShares[0](i);
            b.mShares[1](pos) = b2.mColumns[1].mShares[1](i);

            ++pos;
        }
    }

    BetaLibrary lib;
    auto circ = EqualityCheckAndMuxDepthOptimized();
    Sh3BinaryEvaluator eval;
    eval.setCir(&circ, firstn * firstn, srvs.mEnc.mShareGen);
    eval.setInput(0, tag1);
    eval.setInput(1, a);
    eval.setInput(2, b);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, tag1);

    ColBitAnd(partyIdx, srvs, tag2, tag1);
    
    auto end_time = clock();
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;

}

void prepare_ranks(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "source", TypeID::IntID, 64 }, ColumnInfo{ "original_pi", TypeID::IntID, 64 }, ColumnInfo{ "source_pi", TypeID::IntID, 64 }};
    std::vector<std::vector<u64>> raw_alpha;
    LoadData("../SMQP/graph/bitcoinalpha_tag_pi.csv", raw_alpha);
    u64 alpha_nrows = raw_alpha.size();
    Table alpha(alpha_nrows, ColInfo);
    for (auto i=0; i<alpha_nrows; ++i) {
        raw_alpha[i].erase(raw_alpha[i].begin() + 3); // delete timestamp
    }
    for (auto i=0; i<alpha_nrows; ++i) {
        alpha.mColumns[0].mData(i, 0) = raw_alpha[i][0];
        alpha.mColumns[1].mData(i, 0) = i + 1;
        alpha.mColumns[2].mData(i, 0) = 0;
        
    }

    // sort by source
    SharedTable b;
    if (partyIdx == 0) {
        b = srvs.localInput(alpha);
    } else {
        b = srvs.remoteInput(0);
    }
    // std::cerr << "\nshared alpha \n";
    // STableReveal(srvs, b1, 10);
    // std::cerr << "\n";

    // source, target, rating, source_pi, target_pi
    auto start_time = clock();

    ShuffleQuickSort(partyIdx, srvs, b, 0);
    for (auto i=0; i<b.rows(); ++i) {
        b.mColumns[2].mShares[0](i) = b.mColumns[2].mShares[1](i) = i + 1;
    }
    Permutation perm;

    perm.SSPerm(partyIdx, srvs, b, 1);
    auto end_time = clock();
    
    double comp_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << comp_time << std::endl;
    std::cerr << "comm cost = " << (srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv()) / 1024.0 / 1024.0 << std::endl;


}

int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');

    // L2F(partyIdx);
    // L2FBaseline(partyIdx);

    // L3C(partyIdx);
    // L3CBaseline(partyIdx);

    // L2FS(partyIdx);
    // std::cout << std::endl;
    // L2FSBaseline(partyIdx);

    // L3FS(partyIdx);
    // std::cout << std::endl;
    // L3FSBaseline(partyIdx);

    // L3FS(partyIdx);
    // std::cout << std::endl;
    // L3FSBaseline(partyIdx);

    // L3FScut(partyIdx);

    // CartesianProduct(partyIdx);

    prepare_ranks(partyIdx);
    return 0;
}