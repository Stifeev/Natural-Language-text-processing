#include <stdio.h>
#include <iostream>
#include <string>
#include <set>
#include <filesystem>
#include <vector>
#include <ctime>
#include <algorithm>
#include <omp.h>

namespace fs = std::filesystem;
using std::set;
using fs::path;
using std::vector;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

#define ERROR_HANDLE(call, message, ...)                          \
do                                                                \
{                                                                 \
    if (!(call))                                                  \
    {                                                             \
        fwprintf(stderr, L"[ERROR] " message L"\n", __VA_ARGS__); \
        for (i = 0; i < OMP_NUM_THREADS; i++)                     \
        {                                                         \
            free(buf[i]);                                         \
            free(tokens[i]);                                      \
        }                                                         \
        return 0;                                                 \
    }                                                             \
}                                                                 \
while(0);

#define WARNING_HANDLE(call, message, ...)                          \
do                                                                  \
{                                                                   \
    if (!(call))                                                    \
    {                                                               \
        fwprintf(stdout, L"[WARNING] " message L"\n", __VA_ARGS__); \
        continue;                                                   \
    }                                                               \
}                                                                   \
while(0);

#define INFO_HANDLE(message, ...)                                   \
do                                                                  \
{                                                                   \
     fwprintf(stdout, L"[INFO] " message L"\n", __VA_ARGS__);       \
}                                                                   \
while(0);

// Начальный размер буфера
#define BUF_SIZE 50000 

// Количество потоков
#define OMP_NUM_THREADS 4

set<std::wstring> EXTENSIONS = { L".json", L".jsonlines", L".txt", L".xml" };

int get_tokens(const wchar_t *str,
               wchar_t *tokens, int *tokens_size)
{
    bool end=true;
    int i, j,
        n_tokens=0,
        size=0;

    for (i = 0, j = 0; str[i] != L'\0'; i++)
    {
        if (iswalnum(str[i]) ||
            (!end && str[i] == L'-' && iswalnum(str[i + 1]))
           )
        {
            tokens[j] = str[i];
            j++;
            end = false;
        }
        else if(!end) // Разделитель найден
        {
            tokens[j] = L' ';
            j++;
            end = true;
            n_tokens++;
        }
    }

    tokens[j - 1] = L'\0';
    *tokens_size = j - 1; // без /0
    return n_tokens;
}

int wmain(int argc, wchar_t *argv[])
{
    setlocale(LC_ALL, "Russian");

    wchar_t *buf[OMP_NUM_THREADS], *tokens[OMP_NUM_THREADS], *new_buf;
    int i, 
        n_tokens, buf_size[OMP_NUM_THREADS], offset, len,
        tokens_size;
    
    double total_tokens[OMP_NUM_THREADS], avg_tokens[OMP_NUM_THREADS],
           total_size=0;
    FILE *fpi=NULL, *fpo=NULL;
    path path2corpus = L".", path2tokenscorpus = L"./Токены", 
         path2json, path2tokens;
    vector<std::pair<path, int>> pathes_pairs;
    vector<path> pathes;

    high_resolution_clock::time_point time_start, time_end;
    milliseconds time_span;

    /* Гарантия работы ERROR_HANDLE */
    memset(buf, 0, OMP_NUM_THREADS * sizeof(wchar_t *));
    memset(tokens, 0, OMP_NUM_THREADS * sizeof(wchar_t *));

    /* Разбор входных аргументов */
    for (i = 1; i < argc; i++)
    {
        if (wcscmp(argv[i], L"-i") == 0)
        {
            ERROR_HANDLE(i + 1 < argc, "usage: prog.exe -i path2corpus -o path2tokens")
            path2corpus = argv[i + 1];
            i++;
            continue;
        }
        if (wcscmp(argv[i], L"-o") == 0)
        {
            ERROR_HANDLE(i + 1 < argc, "usage: prog.exe -i path2corpus -o path2tokens")
            path2tokenscorpus = argv[i + 1];
            i++;

            fs::create_directories(path2tokens);
            continue;
        }
    }

    /* Выделение памяти */
    for (i = 0; i < OMP_NUM_THREADS; i++)
    {
        buf_size[i] = BUF_SIZE;

        total_tokens[i] = 0;
        avg_tokens[i] = 0;

        ERROR_HANDLE(buf[i] = (wchar_t *)malloc(buf_size[i] * sizeof(wchar_t)), "Not enough memory");
        ERROR_HANDLE(tokens[i] = (wchar_t *)malloc(buf_size[i] * sizeof(wchar_t)), "Not enough memory");
    }

    /* Сбор путей */
    for (auto iter : fs::recursive_directory_iterator(path2corpus))
    {
        if (iter.is_regular_file() && EXTENSIONS.find(iter.path().extension()) != EXTENSIONS.end())
        {
            pathes_pairs.push_back({ iter.path(), iter.file_size() });
        }
    }

    auto comp = [](const std::pair<path, int> &l, const std::pair<path, int> &r) -> bool
    {
        return l.second < r.second;
    };

    std::sort(pathes_pairs.begin(), pathes_pairs.end(), comp);

    pathes.resize(pathes_pairs.size());

    for (i = 0; i < pathes_pairs.size(); i++)
    {
        pathes[i] = pathes_pairs[i].first;
        total_size += pathes_pairs[i].second;
    }

    time_start = high_resolution_clock::now();

    path *pathes_ptr = pathes.data();
    int n_pathes = pathes.size();
    #pragma omp parallel num_threads(OMP_NUM_THREADS)                                   \
                         shared(buf, tokens, buf_size, pathes_ptr,                      \
                                total_tokens, avg_tokens)                               \
                         private(path2json, path2tokens, i, len, offset,                \
                                 new_buf, n_tokens, tokens_size)                        \
                         firstprivate(n_pathes, path2tokenscorpus, fpo, fpi)
    {
        int id = omp_get_thread_num(),
            id_offset = OMP_NUM_THREADS;

        for (i = id; i < n_pathes; i += id_offset)
        {
            path2json = pathes_ptr[i];
            path2tokens = path2tokenscorpus / (pathes_ptr[i].stem().wstring() + L"_tokens.txt");

            WARNING_HANDLE(fpo = _wfopen(path2tokens.c_str(), L"wt, ccs=UTF-8"), "Can't create %s", path2tokens.c_str());
            WARNING_HANDLE(fpi = _wfopen(path2json.c_str(), L"rt, ccs=UTF-8"), "Can't open %s", path2json.c_str());

            INFO_HANDLE("Thread %d processing %d/%d : %s",
                        id, i + 1, n_pathes,
                        path2json.c_str());

            offset = 0;
            while (fgetws(buf[id] + offset, buf_size[id] - offset, fpi) != NULL)
            {
                len = wcslen(buf[id]);

                if ((len + 1) == buf_size[id] && buf[id][len - 1] != L'\n') // overflow
                {
                    
                    INFO_HANDLE("Reallocating memory for buffer in thread %d", id);

                    WARNING_HANDLE(new_buf = (wchar_t *)realloc(buf[id], 2 * buf_size[id] * sizeof(wchar_t)), 
                                   "Not enough memory to allocate on %d thread", id);
                    buf[id] = new_buf;
                    
                    WARNING_HANDLE(new_buf = (wchar_t *)realloc(tokens[id], 2 * buf_size[id] * sizeof(wchar_t)),
                                   "Not enough memory to allocate on %d thread", id);
                    tokens[id] = new_buf;

                    buf_size[id] *= 2;
                    offset = len;

                    continue;
                }

                offset = 0;

                n_tokens = get_tokens(buf[id], tokens[id], &tokens_size);
                if (n_tokens > 0)
                {
                    fwprintf(fpo, L"%d %d ", n_tokens, tokens_size); // Пишем количество токенов и размер буфера (в wchar'ах) для считывания (без учёта конца строки)
                    fputws(tokens[id], fpo);
                    fputwc(L'\n', fpo);
                }
                total_tokens[id] += n_tokens;
                avg_tokens[id] += (double)tokens_size - n_tokens + 1;
            }

            if (fpo) fclose(fpo);
            if (fpi) fclose(fpi);
        }
    }

    for (i = 1; i < OMP_NUM_THREADS; i++)
    {
        total_tokens[0] += total_tokens[i];
        avg_tokens[0] += avg_tokens[i];
    }

    time_end = high_resolution_clock::now();
    time_span = std::chrono::duration_cast<milliseconds>(time_end - time_start);

    INFO_HANDLE("Total tokens = %.0lf, avg_token = %.2lf\n"         \
                "Total time = %.1lf sec, total size = %.3lf Gb\n"   \
                "Speed = %.3lf ms / Kb",
                total_tokens[0], avg_tokens[0] / total_tokens[0],
                (double)time_span.count() / 1e3 , total_size / (1024. * 1024. * 1024.), 
                (double)time_span.count() / total_size * 1024. * 1024.);
    
    for (i = 0; i < OMP_NUM_THREADS; i++)
    {
        free(buf[i]);
        free(tokens[i]);
    }
    
    return 0;
}
