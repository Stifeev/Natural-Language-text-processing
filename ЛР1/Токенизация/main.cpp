#include <stdio.h>
#include <iostream>
#include <string>
#include <set>
#include <filesystem>
#include <vector>
#include <ctime>

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
        free(buf);                                                \
        free(tokens);                                             \
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

set<std::wstring> EXTENSIONS = { L".json", L".jsonlines", L".txt", L".xml" };

bool is_letter(wchar_t c)
{
    c = towlower(c);
    return L'a' <= c && c <= L'z' ||
           L'а' <= c && c <= L'я' ||
           c == L'ё';
}

bool is_number(wchar_t c)
{
    return L'0' <= c && c <= L'9';
}

int get_tokens(const wchar_t *str,
               wchar_t *tokens, int *tokens_size)
{
    bool end=true;
    int i, j,
        n_tokens=0,
        size=0;

    for (i = 0, j = 0; str[i] != L'\0'; i++)
    {
        if (is_letter(str[i]) || is_number(str[i]) ||
            (!end && str[i] == L'-' && (is_letter(str[i+1]) || is_number(str[i+1])))
           )
        {
            tokens[j] = str[i];
            j++;
            end = false;
        }
        else if(!end) // Разделитель найден
        {
            tokens[j] = ' ';
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

    wchar_t *buf=NULL, *new_buf, *tokens=NULL;
    int i, 
        n_tokens, buf_size=BUF_SIZE, offset, len,
        tokens_size;
    
    double total_tokens=0, avg_tokens=0,
           total_size=0; 
    FILE *fpi=NULL, *fpo=NULL;
    path path2corpus = L".", path2tokenscorpus = L"./Токены", 
         path2json, path2tokens;
    vector<path> pathes;

    high_resolution_clock::time_point time_start, time_end;
    milliseconds time_span;

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

    ERROR_HANDLE(buf = (wchar_t *)malloc(buf_size * sizeof(wchar_t)), "Not enough memory");
    ERROR_HANDLE(tokens = (wchar_t *)malloc(buf_size * sizeof(wchar_t)), "Not enough memory");

    for (auto iter : fs::recursive_directory_iterator(path2corpus))
    {
        if (iter.is_regular_file() && EXTENSIONS.find(iter.path().extension()) != EXTENSIONS.end())
        {
            pathes.push_back(iter.path());
        }
    }

    time_start = high_resolution_clock::now();

    for (i = 0; i < pathes.size(); i++)
    {
        path2json = pathes[i];
        path2tokens = path2tokenscorpus / (pathes[i].stem().wstring() + L"_tokens.txt");

        WARNING_HANDLE(fpo = _wfopen(path2tokens.c_str(), L"wt, ccs=UTF-8"), "Can't create %s", path2tokens.c_str());
        WARNING_HANDLE(fpi = _wfopen(path2json.c_str(), L"rt, ccs=UTF-8"), "Can't open %s", path2json.c_str());

        INFO_HANDLE("Processing %d / %d : %s",
                    i + 1, pathes.size(),
                    path2json.c_str());
        
        offset = 0;
        while (fgetws(buf + offset, buf_size - offset, fpi) != NULL)
        {
            len = wcslen(buf);

            if ((len + 1) == buf_size && buf[len - 1] != L'\n') // overflow
            {
                buf_size *= 2;

                INFO_HANDLE("Reallocate memory for buffer");

                ERROR_HANDLE(new_buf = (wchar_t *)realloc(buf, buf_size * sizeof(wchar_t)), "Not enough memory");
                buf = new_buf;
                ERROR_HANDLE(new_buf = (wchar_t *)realloc(tokens, buf_size * sizeof(wchar_t)), "Not enough memory");
                tokens = new_buf;

                offset = len;
                continue;
            }

            offset = 0;

            total_size += len;
            n_tokens = get_tokens(buf, tokens, &tokens_size);
            if (n_tokens > 0)
            {
                fwprintf(fpo, L"%d %d ", n_tokens, tokens_size); // Пишем количество токенов и размер буфера (в wchar'ах) для считывания (без учёта конца строки)
                fputws(tokens, fpo);
                fputwc(L'\n', fpo);
            }
            total_tokens += n_tokens;
            avg_tokens += (double)tokens_size - n_tokens + 1;
        }

        if (fpo) fclose(fpo);
        if (fpi) fclose(fpi);
    }

    total_size *= sizeof(wchar_t); // Переводим в байты
    time_end = high_resolution_clock::now();
    time_span = std::chrono::duration_cast<milliseconds>(time_end - time_start);

    INFO_HANDLE("Total tokens = %.0lf, avg_token = %.2lf\n"         \
                "Total time = %.1lf sec, total size = %.3lf Gb\n"   \
                "Speed = %.3lf ms / Kb",
                total_tokens, avg_tokens / total_tokens,
                (double)time_span.count() / 1e3 , total_size / (1024. * 1024. * 1024.), 
                (double)time_span.count() / total_size * 1024. * 1024.);
  
    free(buf);
    free(tokens);

    return 0;
}