#ifndef OPERATIONS_H
#define OPERATIONS_H

#include <iostream>
#include <string>
#include <vector>
#include <fstream>
using namespace std;


vector<pair<string, string>> fileToLines(string fileName, string filePath, int startOffset, int endOffset);

vector<vector<int>> divideFile(string filePath, int n);

vector<pair<string, string>> splitString(pair<string, string> line);

pair<string, int> getWordCount(pair<string, string> token);

#endif // OPERATIONS_H