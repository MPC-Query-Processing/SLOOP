#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"
#include "SingleRelationOperator.h"

using namespace oc;
using namespace aby3;

void ShuffleQuickSort(u64 partyIdx, DBServer& srvs, SharedTable &R, u64 sortkeyid, u64 orderby = 1);
// orderby = [0/1] represents ASC/DESC;
