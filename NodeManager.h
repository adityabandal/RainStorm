#ifndef NODEMANAGER_H
#define NODEMANAGER_H

#include <iostream>
#include <string>
#include "Node.h"
#include "HyDFS.h"
#include "Manager.h"
#include "Task/Task.h"
using namespace std;

#define Tuple tuple<string, string, string>

class NodeManager : public Manager {

private:
    Node leaderNode;
    unordered_map<string, unique_ptr<Task>> taskIdToTask;
    atomic<int> totalMsgs;

public:
    NodeManager(char* port, Node selfNode, HyDFS* dfs, Node leaderNode);
    ~NodeManager();
    void startTask(RainStormMessage message);

    
    void messageHandler(int sockfd);

    void executeCommand(string command) override;
    void startListening(int socket) override;
    void membershipUpdate(vector<Node> nodes) override;
    bool runCommandLine(string command) override;

};

#endif // NODEMANAGER_H