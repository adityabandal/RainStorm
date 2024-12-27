#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
using namespace std;

std::vector<std::string> extractColumns(const std::string& line) {
    std::vector<std::string> columns;
    std::stringstream ss(line);
    std::string item;

    // TODO: verify if this works for data conatining commas and quotes

    bool inQuotes = false;
    while (ss.good()) {
        char c = ss.get();
        if (c == '"') {
            inQuotes = !inQuotes;
        } else if (c == ',' && !inQuotes) {
            columns.push_back(item);
            item.clear();
        } else {
            item += c;
        }
    }
    columns.push_back(item); // add the last column

    return columns;
}

int main() {
    std::ifstream inputFile("files/introducer/local/TrafficSigns_10000.csv");
    if (!inputFile) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    string line;
    unordered_map<int, int> columnCntCnt;
    int i = 1;
    while (getline(inputFile, line)) {
        vector<string> columns = extractColumns(line);
        columnCntCnt[columns.size()]++;
        if(columns.size() != 20){
            cout<<i<<" ->"<<columns.size()<<endl;
        }
        if(stoi(columns[2]) != i){
            cout<<i<<" ->"<<columns[2]<<endl;
            i = stoi(columns[2]);
        }
        i++;
        // if(i == 10){
        //     break;
        // }
    }

    for(auto& p: columnCntCnt){
        cout<<p.first<<" : "<<p.second<<endl;
    }

    inputFile.close();
    return 0;
}

