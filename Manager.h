#ifndef MANAGER_H
#define MANAGER_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "HyDFS.h"
#include "Node.h"
#include "RainStormMessage.h"

using namespace std;

class Manager {
public:
    Manager(char* port, Node selfNode, HyDFS* dfs);
    virtual ~Manager();

    
    virtual void start();
    virtual void stop();
    int getStormSocketForNode(Node node);
    int getStormSocketForNode(string nodeId);
    void sendMessageToSocket(int sockfd, RainStormMessage message);
    RainStormMessage receiveMessageFromSocket(int sockfd);
    Node getSelfNode() { return selfNode; }
    virtual void setSelfNode(Node node);
    virtual void executeCommand(string command) = 0;
    virtual void startListening(int socket) = 0;
    virtual void membershipUpdate(vector<Node> nodes) = 0;
    virtual bool runCommandLine(string command) = 0;

protected:
    unordered_map<string, Node> nodeIdToNode;
    std::mutex nodeMapMutex;
    HyDFS* dfs;
    int streamSocket;
    char* streamPort;
    atomic<bool> running;
    Node selfNode;
    vector<thread> threads;

};

#endif // MANAGER_H
