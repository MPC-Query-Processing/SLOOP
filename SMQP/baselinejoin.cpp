#include "baselinejoin.h"

void DegreeCalculation(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, SharedTable &RB, SharedTable &SB) {
    SharedTable TB;
    u64 tnrows = rkey.rows() + skey.rows();

    // Calculate Degree
    // Column 0 - Key
    // COlumn 1 - Current ID
    // Column 2 - Degree-R
    // Column 3 - Degree-S
    TB.mColumns.resize(4);
    TB.mColumns[0].resize(tnrows, 64);
    TB.mColumns[1].resize(tnrows, 64);
    TB.mColumns[2].resize(tnrows, 64);
    TB.mColumns[3].resize(tnrows, 64);

    for (auto i=0; i<rkey.rows(); ++i) {
        TB.mColumns[0].mShares[0](i) = rkey.mShares[0](i);
        TB.mColumns[0].mShares[1](i) = rkey.mShares[1](i);

        TB.mColumns[1].mShares[0](i) = TB.mColumns[1].mShares[1](i) = i;
        TB.mColumns[2].mShares[0](i) = TB.mColumns[2].mShares[1](i) = 1;
        TB.mColumns[3].mShares[0](i) = TB.mColumns[3].mShares[1](i) = 0;
    }

    for (auto i=0; i<skey.rows(); ++i) {
        TB.mColumns[0].mShares[0](i + rkey.rows()) = skey.mShares[0](i);
        TB.mColumns[0].mShares[1](i + rkey.rows()) = skey.mShares[1](i);

        TB.mColumns[1].mShares[0](i + rkey.rows()) = TB.mColumns[1].mShares[1](i + rkey.rows()) = i + rkey.rows();
        TB.mColumns[2].mShares[0](i + rkey.rows()) = TB.mColumns[2].mShares[1](i + rkey.rows()) = 0;
        TB.mColumns[3].mShares[0](i + rkey.rows()) = TB.mColumns[3].mShares[1](i + rkey.rows()) = 1;

    }
    ShuffleQuickSort(partyIdx, srvs, TB, 0);

    SegmentedPrefixSum(partyIdx, srvs, TB.mColumns[0], TB.mColumns[2]);
    SegmentedPrefixSum(partyIdx, srvs, TB.mColumns[0], TB.mColumns[3]);
    SegmentedCopy(partyIdx, srvs, TB.mColumns[0], TB.mColumns[2], TB.mColumns[3]);

    Permutation perm;
    perm.SSPerm(partyIdx, srvs, TB, 1, 0);

    RB.mColumns.resize(3);
    RB.mColumns[0].resize(rkey.rows(), 64);
    RB.mColumns[1].resize(rkey.rows(), 64);
    RB.mColumns[2].resize(rkey.rows(), 64);

    for (auto i=0; i<rkey.rows(); ++i) {
        RB.mColumns[0].mShares[0](i) = TB.mColumns[0].mShares[0](i);
        RB.mColumns[0].mShares[1](i) = TB.mColumns[0].mShares[1](i);
        RB.mColumns[1].mShares[0](i) = TB.mColumns[2].mShares[0](i);
        RB.mColumns[1].mShares[1](i) = TB.mColumns[2].mShares[1](i);
        RB.mColumns[2].mShares[0](i) = TB.mColumns[3].mShares[0](i);
        RB.mColumns[2].mShares[1](i) = TB.mColumns[3].mShares[1](i);
    }


    SB.mColumns.resize(3);
    SB.mColumns[0].resize(skey.rows(), 64);
    SB.mColumns[1].resize(skey.rows(), 64);
    SB.mColumns[2].resize(skey.rows(), 64);

    for (auto i=0; i<skey.rows(); ++i) {
        SB.mColumns[0].mShares[0](i) = TB.mColumns[0].mShares[0](i + rkey.rows());
        SB.mColumns[0].mShares[1](i) = TB.mColumns[0].mShares[1](i + rkey.rows());
        SB.mColumns[2].mShares[0](i) = TB.mColumns[2].mShares[0](i + rkey.rows());
        SB.mColumns[2].mShares[1](i) = TB.mColumns[2].mShares[1](i + rkey.rows());
        SB.mColumns[1].mShares[0](i) = TB.mColumns[3].mShares[0](i + rkey.rows());
        SB.mColumns[1].mShares[1](i) = TB.mColumns[3].mShares[1](i + rkey.rows());
    }

    // STableReveal(srvs, RB);
    // std::cout << std::endl;;
    // STableReveal(srvs, SB);
    // std::cout << std::endl;;
}


void PSIwithPayloadBaseline(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload) {
    SharedTable TB;
    u64 tnrows = rkey.rows() + skey.rows();

    // Calculate Degree
    // Column 0 - Key
    // COlumn 1 - Current ID
    // Column 2 - Payload
    TB.mColumns.resize(3);
    TB.mColumns[0].resize(tnrows, 64);
    TB.mColumns[1].resize(tnrows, 64);
    TB.mColumns[2].resize(tnrows, 64);

    for (auto i=0; i<rkey.rows(); ++i) {
        TB.mColumns[0].mShares[0](i) = rkey.mShares[0](i);
        TB.mColumns[0].mShares[1](i) = rkey.mShares[1](i);

        TB.mColumns[1].mShares[0](i) = TB.mColumns[1].mShares[1](i) = i;
        TB.mColumns[2].mShares[0](i) = TB.mColumns[2].mShares[1](i) = 0;
    }

    for (auto i=0; i<skey.rows(); ++i) {
        TB.mColumns[0].mShares[0](i + rkey.rows()) = skey.mShares[0](i);
        TB.mColumns[0].mShares[1](i + rkey.rows()) = skey.mShares[1](i);

        TB.mColumns[1].mShares[0](i + rkey.rows()) = TB.mColumns[1].mShares[1](i + rkey.rows()) = i + rkey.rows();
        TB.mColumns[2].mShares[0](i + rkey.rows()) = spayload.mShares[0](i);
        TB.mColumns[2].mShares[1](i + rkey.rows()) = spayload.mShares[1](i);

    }
    ShuffleQuickSort(partyIdx, srvs, TB, 0, 0);
    SegmentedCopy(partyIdx, srvs, TB.mColumns[0], TB.mColumns[2]);

    Permutation perm;
    perm.SSPerm(partyIdx, srvs, TB, 1, 0);

    rpayload.resize(rkey.rows(), 64);

    for (auto i=0; i<rkey.rows(); ++i) {
        rpayload.mShares[0](i) = TB.mColumns[2].mShares[0](i);
        rpayload.mShares[1](i) = TB.mColumns[2].mShares[1](i);
    }
}

void BinaryJoinBaseline(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, std::vector<u64> RCalPIID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID,
    SharedTable &T) {
    
    Permutation perm;

    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBPIKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBPIKeyID);
    }
    std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    DegreeCalculation(partyIdx, srvs, R.mColumns[RBKeyID], S.mColumns[SBKeyID], RB, SB);

    // Update PI
    if (RCalPIID.size() != 0) {
        for (auto piid : RCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(rnrows, 64);
            PItable.mColumns[0].mShares = R.mColumns[RBPIKeyID].mShares;
            PItable.mColumns[1].resize(rnrows, 64);
            PItable.mColumns[1].mShares = RB.mColumns[2].mShares;
            PItable.mColumns[2].resize(rnrows, 64);
            PItable.mColumns[2].mShares = R.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (rnrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<rnrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            R.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    if (SCalPIID.size() != 0) {
        for (auto piid : SCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(snrows, 64);
            PItable.mColumns[0].mShares = S.mColumns[SBPIKeyID].mShares;
            PItable.mColumns[1].resize(snrows, 64);
            PItable.mColumns[1].mShares = SB.mColumns[2].mShares;
            PItable.mColumns[2].resize(snrows, 64);
            PItable.mColumns[2].mShares = S.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (snrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<snrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            S.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    // Expansion
    SharedTable RE, SE;
    // STableReveal(srvs, R);
    Expansion(partyIdx, srvs, R, RB.mColumns[2], RE, -1);
    std::cout << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);

    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    for (auto i=0; i<S.rows(); ++i) {
        S.mColumns[snattrs].mShares[0](i) = S.mColumns[snattrs].mShares[1](i) = 1;
    }

    S.mColumns.resize(snattrs + 2);
    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;
    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, -1, true, SBPIKeyID, SBKeyID);
    std::cout << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE);

    if (RCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : RCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, RE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, RE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            RE.mColumns[piid].mShares = addone.mShares;
        }
    }

    if (SCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : SCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, SE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, SE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            SE.mColumns[piid].mShares = addone.mShares;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    // std::cout << partyIdx << ",CalcAlignmentPI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    perm.SSPerm(partyIdx, srvs, SE, SE.mColumns.size() - 1, 1);
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
        if (i == RBPIKeyID) {
            for (auto j=0; j<RE.rows(); ++j) {
                T.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[1](j) = j + 1;
            }
        }
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }
    return;
}

void PKJoinBaseline(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, // R is the table with foreign key (R dont need expansion, nor recalculation PI)
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, // S is the table with primary key
    SharedTable &T) {
    
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID);
    }
    // std::cout << partyIdx << ",SORT-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();


    // Update PI
    if (SCalPIID.size() != 0) {
        for (auto piid : SCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(snrows, 64);
            PItable.mColumns[0].mShares = S.mColumns[SBPIKeyID].mShares;
            PItable.mColumns[1].resize(snrows, 64);
            PItable.mColumns[1].mShares = SB.mColumns[2].mShares;
            PItable.mColumns[2].resize(snrows, 64);
            PItable.mColumns[2].mShares = S.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (snrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<snrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            S.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    // Expansion
    SharedTable RE = R, SE;
    // STableReveal(srvs, R);
    // std::cout << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);
    DegreeCalculation(partyIdx, srvs, R.mColumns[RBKeyID], S.mColumns[SBKeyID], RB, SB);
    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    for (auto i=0; i<S.rows(); ++i) {
        S.mColumns[snattrs].mShares[0](i) = S.mColumns[snattrs].mShares[1](i) = 1;
    }

    S.mColumns.resize(snattrs + 2);
    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;
    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, -1, true, SBPIKeyID, SBKeyID);
    // std::cout << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE);

    if (SCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : SCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, SE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, SE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            SE.mColumns[piid].mShares = addone.mShares;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    perm.SSPerm(partyIdx, srvs, SE, SE.mColumns.size() - 1, 1);
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
        if (i == RBPIKeyID) {
            for (auto j=0; j<RE.rows(); ++j) {
                T.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[1](j) = j + 1;
            }
        }
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }
    return;
}


void SemiJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T) {
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID, 0);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID, 0);
    }
    // std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    SB.mColumns.resize(2);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    SB.mColumns[1].mShares = S.mColumns[SAggKeyID].mShares;

    // Aggregate function 
    if (AFunType == AggFunc::SUM) {
        SegmentedPrefixSum(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    } else if(AFunType == AggFunc::EXISTS) {
        SegmentedPrefixOr(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    } else {
        std::cout << "Current function not support!\n";
        assert(0);
    }

    sbMatrix RBpayload;

    PSIwithPayloadBaseline(partyIdx, srvs, R.mColumns[RBKeyID], SB.mColumns[0], SB.mColumns[1], RBpayload);
    // SegmentedCopy(partyIdx, srvs, R.mColumns[RBKeyID], RBpayload);
    // PKFKwithPayload(partyIdx, srvs, R.mColumns[RBKeyID], SB.mColumns[0], SB.mColumns[1], RBpayload);
    // std::cout << "finished \n";

    u64 nattrs = R.mColumns.size();
    T.mColumns.resize(nattrs + 1);
    for (auto i=0; i<nattrs; ++i) {
        T.mColumns[i].mShares = R.mColumns[i].mShares;
    }
    T.mColumns[nattrs].mShares = RBpayload.mShares;
    return;
}

void BinaryJoinBaselineWD(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RTagID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 STagID,
    SharedTable &T, i64 nout) {
    
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID, 0);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID, 0);
    }
    S.mColumns.resize(S.mColumns.size() + 1);
    u64 SBPIKeyID = S.mColumns.size() - 1;
    S.mColumns[S.mColumns.size() - 1].resize(S.rows(), 64);
    for (auto i=0; i<S.rows(); ++i) {
        S.mColumns[S.mColumns.size() - 1].mShares[0](i) = S.mColumns[S.mColumns.size() - 1].mShares[1](i) = i+1;
    }
    // STableReveal(srvs, S, 10);
    std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    // Calculate Degree
    // Column 0 - Key
    // Column 1 - Degree
    RB.mColumns.resize(3);
    RB.mColumns[0].resize(rnrows, 64);
    RB.mColumns[1].resize(rnrows, 64);
    RB.mColumns[2].resize(rnrows, 64);
    RB.mColumns[0].mShares = R.mColumns[RBKeyID].mShares;
    RB.mColumns[1].mShares = R.mColumns[RTagID].mShares; // degree is set to tag initially
    SegmentedPrefixSum(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1]);

    SB.mColumns.resize(3);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[2].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    SB.mColumns[1].mShares = S.mColumns[STagID].mShares;
    SegmentedPrefixSum(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    // Add Column "J" <- SB.mColumns[1]
    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    S.mColumns[snattrs].mShares = SB.mColumns[1].mShares;
    std::cout << partyIdx << ",DEG-PREFIX-SUM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // std::cout << "before PSI with payload, COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    PSIwithPayload(partyIdx, srvs, RB.mColumns[0], SB.mColumns[0], SB.mColumns[1], RB.mColumns[2]);
    PSIwithPayload(partyIdx, srvs, SB.mColumns[0], RB.mColumns[0], RB.mColumns[1], SB.mColumns[2]);
    std::cout << partyIdx << ",PSI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SegmentedCopy(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1], RB.mColumns[2]);
    SegmentedCopy(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1], SB.mColumns[2]);
    std::cout << partyIdx << ",COPY-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    ColBitAnd(partyIdx, srvs, R.mColumns[RTagID], RB.mColumns[2]);
    ColBitAnd(partyIdx, srvs, S.mColumns[STagID], SB.mColumns[2]);

    // Expansion
    SharedTable RE, SE;
    // STableReveal(srvs, R);
    Expansion(partyIdx, srvs, R, RB.mColumns[2], RE, nout);
    std::cout << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);

    S.mColumns.resize(snattrs + 2);

    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;

    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, nout, true, SBPIKeyID, SBKeyID);
    std::cout << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE, 10);

    // Regenerate Tag
    sb64 realout;
    if (nout != -1) {
        sbMatrix agg_deg = RB.mColumns[2];
        PrefixSum(partyIdx, srvs, agg_deg);
        u64 nrows = agg_deg.rows();
        realout.mData[0] = agg_deg.mShares[0](nrows - 1);
        realout.mData[1] = agg_deg.mShares[1](nrows - 1);

        sbMatrix compa(nout, 64), compb(nout, 64), compr(nout, 1);
        for (auto i=0; i<nout; ++i) {
            compa.mShares[0](i) = compa.mShares[1](i) = i;
            compb.mShares[0](i) = realout.mData[0];
            compb.mShares[1](i) = realout.mData[1];
        }
        auto circ = LessThan(64);
        Sh3BinaryEvaluator eval;
        eval.setCir(&circ, nout, srvs.mEnc.mShareGen);
        eval.setInput(0, compa);
        eval.setInput(1, compb);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, compr);

        for (auto i=0; i<nout; ++i) {
            RE.mColumns[RTagID].mShares[0](i) = compr.mShares[0](i) & 1;
            RE.mColumns[RTagID].mShares[1](i) = compr.mShares[1](i) & 1;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    std::cout << partyIdx << ",CalcAlignmentPI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    
    // STableReveal(srvs, SE, 80000);

    sbMatrix runindex(SE.rows(), 64);
    for (auto i=0; i<runindex.rows(); ++i) {
        runindex.mShares[0](i) = runindex.mShares[1](i) = i+1;
    }
    BetaLibrary lib;
    auto circ = ZeroCheckAndMux();
    Sh3BinaryEvaluator eval;
    u64 sepiid = SE.mColumns.size() - 1;
    // eval.setCir(&circ, SE.rows(), srvs.mEnc.mShareGen);
    // eval.setInput(0, SE.mColumns[RTagID]);
    // eval.setInput(1, runindex);
    // eval.setInput(2, SE.mColumns[sepiid]);
    // eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    // eval.getOutput(0, SE.mColumns[sepiid]);
    SE.mColumns[sepiid].mShares = runindex.mShares;

    perm.SSPerm(partyIdx, srvs, SE, sepiid, 1);
    SE.mColumns.pop_back();
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }

    return;
}
