#ifndef HYDFS_H
#define HYDFS_H

#include <string>
#include <map>
#include <vector>
#include <thread>
#include <atomic>
#include "Node.h"
#include "HyDFSMessage.h"
#include "ConsistentHashRing.h"
#include "FileMetaData.h"
using namespace std;
class HyDFS {
private:
    ConsistentHashRing ring;
    string selfNodeId;
    char* tcpPort;
    int tcpSocket;
    string filesDirectory;
    atomic<int> threadsRunning;
    vector<thread> threads;
    FileList primaryFilsList, secondaryFilesList;
    atomic<int> bytesReceived, bytesSent;
    atomic<int> totalBytesReceived, totalBytesSent;
    string startEpoch;


    // void handleClient(int clientSockfd);
    // void tcpListener(int tcpSockfd);

public:
    HyDFS(char *port);
    ~HyDFS();

    bool runCommand(std::string command);
    void membershipUpdate(std::vector<Node>& nodes);
    void updateRing(Node node);
    void setSelfNode(Node node);
    void start();
    void stopThreads();

    // command handlers
    void listMyFiles();
    void requestFile(string fileName, string localFileName);
    void createFile(string localFilename, string hydfsFilename);
    void appendFile(string fileName, string localFileName);
    void mergeFile(string fileName);
    void listNodesHavingFile(string fileName);
    void multiAppend(string hydfsFileName, int numOfAppends, int numOfClients);
    void getFileFromReplica(string vmId, string hydfsFileName, string localFileName);

    // message handlers
    void getFileHandler(HyDFSMessage message, int clientSockfd);
    void putFileHandler(HyDFSMessage message,int clientSockfd);
    void multiAppendHandler(HyDFSMessage message);
    void putFileHelper(string fileName, int version, int clientSockfd);
    void appendFileHandler(HyDFSMessage message, int clientSockfd);
    void getPrimaryFileListHandler(HyDFSMessage message, int clientSockfd);
    void getFileAllListHandler(HyDFSMessage message, int clientSockfd);
    void getShardHandler(HyDFSMessage message, int clientSockfd);
    void mergeFileHandler(HyDFSMessage message);
    void getAllFileListHandler(HyDFSMessage message, int clientSockfd);
    

    // threads
    void tcpListener(int tcpSockfd); 
    void handleClient(int clientSockfd); 
    void predecessorPinger(); 
    void monitorFileLists();


    // utility functions
    void sendFileToNode(string filepath, Node node, string hydfsFilename, int version);
    void appendFileToNode(string filepath, Node node, string hydfsFileName);
    int sendMessageToNode(HyDFSMessage message, Node node);
    void sendMessageToSocket(HyDFSMessage message, int sockfd);
    void sendFileToSocket(string filepath, int sockfd);
    void setLocalFilesPath(string path);
    void receiveFileFromNode(string filepath, Node node, string hydfsFilename);
    bool receiveFileFromSocket(string filepath, int sockfd);
    int getSocketForNode(Node node);
    HyDFSMessage receiveMessageFromSocket(int sockfd);
    void transferFileToNode(string fileName, Node node, bool moveToReplica);
    void mergeFile(string fileName, int numberOfShards, Node fromNode);
    void mergeAllReplicas();
    unordered_set<string> getAllFileListFromNode(Node node);
    void printBandwidth();
    void printCurrentBandwidth(double ms);
    string getLocalFilesPath(string filename);
    string getHyDFSFilesPath(string filename, int version);
    bool isFilePresentInHyDFS(string filename);
    void createEmptyFile(string filename);
};

#endif // HYDFS_H