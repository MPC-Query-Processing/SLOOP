#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"
#include "SingleRelationOperator.h"
#include "sort.h"
#include "join.h"

using namespace oc;
using namespace aby3;

void DegreeCalculation(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, SharedTable &RB, SharedTable &SB);

void PSIwithPayloadBaseline(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload);

void BinaryJoinBaseline(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, std::vector<u64> RCalPIID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID,
    SharedTable &T);

void BinaryJoinBaselineWD(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RTagID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 STagID,
    SharedTable &T, i64 nout = -1);

void PKJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, SharedTable &T);

void SemiJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T);
