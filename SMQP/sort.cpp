#include "sort.h"

void ShuffleQuickSort(u64 partyIdx, DBServer& srvs, SharedTable &R, u64 sortkeyid, u64 orderby) {
    u64 neles = R.rows();

    u64 ncols = R.mColumns.size();
    R.mColumns.resize(ncols + 1);
    R.mColumns[ncols].resize(neles, 64);
    for (auto i=0; i<neles; ++i) {
        R.mColumns[ncols].mShares[0](i) = R.mColumns[ncols].mShares[1](i) = i;
    }

    auto start = clock();
    Permutation perm;
    perm.RandPerm(partyIdx, srvs, R);
    auto end = clock();
    // std::cout << "RandPerm, TIME = " << 1.0 * (end - start) / CLOCKS_PER_SEC << std::endl;

    std::vector<std::pair<u64, u64>> intervals = {std::make_pair((u64)0, neles-1)}, newintervals;
    u64 rounds = 0;

    std::vector<u64> indices (neles);
    for (auto i=0; i<neles; ++i) {
        indices[i] = i;
    }

    BetaLibrary lib;
    auto circ = LessThan(128);
    Sh3BinaryEvaluator eval;
    sbMatrix compa(neles, 128), compb(neles, 128), compr(neles, 1);
    while (true) {
        bool flag = true;
        for (auto inter : intervals) {
            u64 left = inter.first, right = inter.second;
            if (left < right) {
                flag = false;
                for (auto i=left+1; i<=right; ++i) {
                    compa.mShares[0](i, 1) = R.mColumns[sortkeyid].mShares[0]( indices[i] );
                    compa.mShares[1](i, 1) = R.mColumns[sortkeyid].mShares[1]( indices[i] );
                    compa.mShares[0](i, 0) = R.mColumns[ncols].mShares[0]( indices[i] );
                    compa.mShares[1](i, 0) = R.mColumns[ncols].mShares[1]( indices[i] );

                    compb.mShares[0](i, 1) = R.mColumns[sortkeyid].mShares[0]( indices[left] );
                    compb.mShares[1](i, 1) = R.mColumns[sortkeyid].mShares[1]( indices[left] );
                    compb.mShares[0](i, 0) = R.mColumns[ncols].mShares[0]( indices[left] );
                    compb.mShares[1](i, 0) = R.mColumns[ncols].mShares[1]( indices[left] );
                }
            }
        }

        if (flag) {
            break;
        }

        // SBMatReveal(srvs, compa);
        // SBMatReveal(srvs, compb);
        eval.setCir(&circ, neles, srvs.mEnc.mShareGen);
        eval.setInput(0, compa);
        eval.setInput(1, compb);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, compr);

        // SBMatReveal(srvs, compr);
        aby3::i64Matrix out(compr.rows(), compr.i64Cols());
        srvs.mEnc.revealAll(srvs.mRt.mComm, compr, out);

        newintervals.clear();
        for (auto inter : intervals) {
            u64 left = inter.first, right = inter.second;
            if (left < right) {
                // std::cerr << "range:  " << left << ' ' << right << std::endl;
                // for (auto i=left+1; i<=right; ++i) {
                //     std::cerr << out(i) << ' ';
                // }
                // std::cerr << std::endl;
                uint32_t i = left+1, j = right;
                while (i <= j) {
                    while (i <= j && out(i) == 1) ++i;
                    while (i <= j && out(j) == 0) --j;
                    if (i < j) {
                        std::swap(indices[i], indices[j]);
                        i += 1;
                        j -= 1;
                    }
                }
                i -= 1;
                if (i != left) {
                    std::swap(indices[i], indices[left]);
                }
                // std::cerr << " ==> " << left << ' ' << i << ' ' << right << std::endl;
                if (left != i) newintervals.push_back(std::make_pair(left, i-1));
                if (i != right) newintervals.push_back(std::make_pair(i+1, right));
            }
        }

        intervals = newintervals;
        rounds += 1;
    }
    std::cerr << neles << ' ' << rounds << std::endl;
    
    R.mColumns.pop_back();
    SharedTable temp = R;
    if (orderby == 0) {
        for (auto i=0; i<neles; ++i) {
            for (auto j=0; j<R.mColumns.size(); ++j) {
                R.mColumns[j].mShares[0]( i ) = temp.mColumns[j].mShares[0]( indices[i] );
                R.mColumns[j].mShares[1]( i ) = temp.mColumns[j].mShares[1]( indices[i] );
            }
        }
    } else {
        for (auto i=0; i<neles; ++i) {
            for (auto j=0; j<R.mColumns.size(); ++j) {
                R.mColumns[j].mShares[0]( neles - 1 - i ) = temp.mColumns[j].mShares[0]( indices[i] );
                R.mColumns[j].mShares[1]( neles - 1 - i ) = temp.mColumns[j].mShares[1]( indices[i] );
            }
        }
    }
    
    // STableReveal(srvs, R);
}