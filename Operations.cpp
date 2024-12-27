#include "Operations.h"
#include <sstream>


vector<pair<string, string>> fileToLines(string fileName, string filePath, int startOffset, int endOffset) {
    // return vector of lines from file from startOffset to endOffset in the form of <fileName:linenumber, lines>
    // start line number with the actual line number of startOffset
    ifstream file;
    vector<pair<string, string>> lines;
    string keyPrefix = fileName + ":";
    file.open(filePath, ios::binary);
    if (!file.is_open()) {
        cerr << "Error opening file: " << filePath << endl;
        return lines;
    }

    // count number of lines till startOffset
    int lineNumber = 1;
    string line;
    while (getline(file, line) && file.tellg() < endOffset) {
        if (file.tellg() >= startOffset) {
            lines.push_back({keyPrefix+to_string(lineNumber), line});
        }
        lineNumber++;
    }
    return lines;
}

vector<vector<int>> divideFile(string filePath, int n) {
    ifstream file;
    vector<vector<int>> offsets;
    file.open(filePath, ios::binary);
    if (!file.is_open()) {
        cerr << "Error opening file: " << filePath << endl;
        return offsets;
    }

    file.seekg(0, ios::end);
    int fileSize = file.tellg();
    int chunkSize = fileSize / n;
    int remainder = fileSize % n;

    int startOffset = 0;
    int endOffset = 0;
    int startLineNumber = 1;
    file.seekg(0, ios::beg);

    for (int i = 0; i < n; ++i) {
        startOffset = endOffset;
        endOffset = startOffset + chunkSize + (i < remainder ? 1 : 0);

        // Adjust endOffset to the end of the line
        file.seekg(endOffset, ios::beg);
        string line;
        if (getline(file, line)) {
            endOffset = file.tellg();
        }

        offsets.push_back({startOffset, endOffset, startLineNumber});

        // Count the number of lines in this chunk
        int lineCount = 0;
        file.seekg(startOffset, ios::beg);
        while (file.tellg() < endOffset && getline(file, line)) {
            lineCount++;
        }
        startLineNumber += lineCount;
    }

    return offsets;
}

vector<pair<string, string>> splitString(pair<string, string> line) {
    // return vector of words from the line in the form of <fileName:linenumber:wordnumber, word>
    vector<pair<string, string>> words;
    string keyPrefix = line.first + ":";
    istringstream iss(line.second);
    string word;
    int wordNumber = 1;
    while (iss >> word) {
        words.push_back({keyPrefix+to_string(wordNumber), word});
        wordNumber++;
    }
    return words;
}

pair<string, int> getWordCount(pair<string, string> token) {
    // return <word, 1>
    return {token.second, 1};
}
