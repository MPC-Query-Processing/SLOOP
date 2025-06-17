# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import matplotlib as mpl 
mpl.use('TkAgg')       

def draw():
    k = [3, 4, 5, 6] #, 50000]
    our_time = [93.6448,25.7469,11.8445,5.03701]
    our_comm = np.array([7727721537,2186384252,998561433,374062218]) / 1024 / 1024
    
    plain_time = [1.15,0.45,0.27,0.1]
    plain_comm = np.array([30141200,9255856,4778832,2418224]) / 1024 / 1024
    fig, axes = plt.subplots(1, 2, figsize=(13, 3))


    axes[0].tick_params(axis='both', which='major', labelsize=10)
    axes[0].set_facecolor("white")
    axes[0].plot(k, our_time,ls="dotted",c=plt.cm.tab20c(1), marker = '*',markersize = 7, lw=1.2,label='LINQ')
    axes[0].plot(k, plain_time,ls="dashed",c=plt.cm.tab20c(9), marker = 's',markersize = 5, lw=1.2,label='PostgreSQL')

    axes[0].set_yscale('log')
    axes[0].set_xticks(k)
    axes[0].set_xticklabels([r"$8.87 \times 10^5$", r'$2.35 \times 10^5$', r'$9.49 \times 10^4$', r'$2.12 \times 10^4$'], fontsize = 10)
    
    axes[1].tick_params(axis='both', which='major', labelsize=10)
    axes[1].set_facecolor("white")
    axes[1].plot(k, our_comm,ls="dotted",c=plt.cm.tab20c(1), marker = '*',markersize = 7, lw=1.2,label='LINQ')
    axes[1].plot(k, plain_comm,ls="dashed",c=plt.cm.tab20c(9), marker = 's',markersize = 5, lw=1.2,label='PostgreSQL')

    axes[1].set_yscale('log')
    axes[1].set_xticks(k)
    axes[1].set_xticklabels([r"$8.87 \times 10^5$", r'$2.35 \times 10^5$', r'$9.49 \times 10^4$', r'$2.12 \times 10^4$'], fontsize = 10)
    

    axes[0].legend(loc='upper right', ncol = 2, fontsize=10)
    axes[0].set_ylabel("Time cost (s)",fontsize=10)
    axes[1].set_ylabel("Communication cost (MB)",fontsize=10)
    axes[0].set_xlabel("#Output rows",fontsize=10)
    axes[1].set_xlabel("#Output rows",fontsize=10)

    plt.savefig("draw.pdf", bbox_inches='tight')


if __name__ == "__main__":
	draw()
