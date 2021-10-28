# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 23:57:39 2021

@author: stife
"""

import struct
import ctypes
from os import path

PATH_2_INDEX = "D:/Мои документы/Лабы и рефераты/5 курс 1 семестр/Информационный поиск/Корпус_index"

path2terms = path.join(PATH_2_INDEX, "terms.data")
path2docs_id = path.join(PATH_2_INDEX, "docs_id.data")
path2postings_list = path.join(PATH_2_INDEX, "postings_list.data")

sizeofint = 4
sizeofuint = 4
sizeofwchar_t = 2

#%% Чтение термов

with open(path2terms, "rb") as fp:
    n_terms, = struct.unpack("I", fp.read(sizeofuint))
    
    terms = [""] * n_terms
    offsets = [0] * n_terms
    
    for i in range(n_terms):
        n_chars, = struct.unpack("I", fp.read(sizeofuint))
        term = ctypes.create_unicode_buffer(n_chars)
        
        fp.readinto(term)
        
        terms[i] = term.value
        
        offset, = struct.unpack("I", fp.read(sizeofuint))
        
        offsets[i] = offset
        
#%% Чтение списков и общего числа документов

total_freq = [0] * n_terms

with open(path2postings_list, "rb") as fp:
    n_terms, = struct.unpack("I", fp.read(sizeofuint))
    
    for i in range(n_terms):
        n_docs, = struct.unpack("I", fp.read(sizeofuint))
        
        docs_id = (ctypes.c_int * n_docs)()
        docs_freq = (ctypes.c_int * n_docs)()
        
        fp.readinto(docs_id)
        fp.readinto(docs_freq)
        
        total_freq[i] = sum(docs_freq[:])
        
with open(path2docs_id, "rb") as fp:
    n_docs, = struct.unpack("I", fp.read(sizeofuint))
    
#%% Сортировка

from matplotlib import pyplot as plt
import numpy as np

term_freq = list(zip(terms, total_freq))

term_freq.sort(key = lambda x: x[1], reverse=True)

rank = np.array(range(1, n_terms + 1), dtype=np.float64)
freq = np.array([_[1] for _ in term_freq], dtype=np.float64)

#%% График Ципфа

plt.figure(figsize=(7.20, 4.80), dpi=100)

k, c = np.linalg.solve([[np.sum(1 / rank ** 2), np.sum(1 / rank)],
                        [np.sum(1 / rank), np.float64(len(rank))]],
                       [np.dot(freq, 1 / rank), np.sum(freq)])

plt.plot(rank, freq, "o", markersize=1, label="Зависимость частоты от ранга")
plt.plot(rank, k / rank + c, label="Закон Ципфа")

plt.xlabel("rank")
plt.ylabel("freq")

plt.xscale("log", basex=10)
plt.yscale("log", basey=10)
plt.legend()
plt.grid(True)

#%% Мандельброт вычисление параметров

from scipy.optimize import minimize

def f(x):
    P, rho, B = x
    return np.sum((np.abs(P * (rank + rho) ** (-B) - freq)), dtype=np.float64)

x0 = np.array([12048049.10105591, 100, 2], dtype=np.float64)

res = minimize(f, x0, method="Nelder-Mead",
               options={"disp": True}, callback=lambda x: print(x),
               tol=1e-4)

P, rho, B = res.x

#%% График Мандельброта

plt.figure(figsize=(7.20, 4.80), dpi=100)

plt.plot(rank, freq, "o", markersize=1, label="Зависимость частоты от ранга")
plt.plot(rank, P * (rank + rho) ** (-B), label="Закон Мандельброта")

plt.xlabel("rank")
plt.ylabel("freq")

plt.xscale("log", basex=10)
plt.yscale("log", basey=10)
plt.legend()
plt.grid(True)