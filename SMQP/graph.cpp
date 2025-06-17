/*
select b1.source, b1.target, b2.target, b3.target from 
bitcoinalpha as b1, bitcoinalpha as b2, bitcoinalpha as b3 
where b1.target = b2.source and b2.target = b3.source 
and b1.rating>=3 
and b2.rating>=3
and b3.rating>=3;

select count(*) from 
bitcoinalpha as b1, bitcoinalpha as b2, bitcoinalpha as b3 
where b1.target = b2.source and b2.target = b3.source 
and b1.rating>=2
and b2.rating>=2
and b3.rating>=2;
*/


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


void L3F(u64 partyIdx, u64 boundk) {
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
        alpha.mColumns[2].mData(i, 0) = (alpha.mColumns[2].mData(i, 0) >= 11 + boundk);
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
    u64 nout = 0;
    if (boundk == 2) {
        nout = 3817909;
    } else if (boundk == 3) {
        nout = 887494;
    } else if (boundk == 4) {
        nout = 234827;
    } else if (boundk == 5) {
        nout = 94920;
    } else if (boundk == 6) {
        nout = 21151;
    }
    std::cout << nout << std::endl;

    std::vector<u64> b1_remaining_cols = {0, 1, 2, 5};
    std::vector<u64> b2_remaining_cols = {0, 1, 2, 4, 5};
    std::vector<u64> b3_remaining_cols = {0, 1, 2, 4};

    TakeColumns(b1, b1_remaining_cols);
    TakeColumns(b2, b2_remaining_cols);
    TakeColumns(b3, b3_remaining_cols);

    BinaryJoinWD(partyIdx, srvs, b2, 1, -1, 4, 2, {3}, b3, 0, -1, 3, 2, {3}, b23, nout);
    // STableReveal(srvs, b23, 10); std::cout << std::endl;
    // source, target, rating, time, source_pi, target_pi, source, target, rating, time, source_pi, target_pi
    // source, target, rating, source_pi, target_pi, source, target, rating, source_pi

    std::vector<u64> col_remaining = {0, 1, 6, 2, 3};
    TakeColumns(b23, col_remaining);
    STableReveal(srvs, b23, 10); std::cout << std::endl;

    BinaryJoinWD(partyIdx, srvs, b1, 1, -1, 3, 2, {}, b23, 0, -1, 4, 3, {}, b123);

    // STableReveal(srvs, b123, 10);

    auto end_time = clock();

    double bj_time = 1.0 * (end_time - start_time) / CLOCKS_PER_SEC;

    std::cerr << "time cost = " << bj_time << std::endl;
    std::cerr << "comm cost = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << std::endl;
}


int main(int argc, char** argv) {
    if (argc == 0) return 0;
    u64 partyIdx = (argv[1][0] - '0');
    u64 boundk = (argv[2][0] - '0');

    L3F(partyIdx, boundk);
    std::cout << std::endl;
    return 0;
}