#include "Manager.h"
#include "ResourceManager.h"
#include "Daemon.cpp"
#include "NodeManager.h"
#include <bits/stdc++.h>

using namespace std;


int main (int argc, char* argv[]) {
    // parse command line arguments and pass to Daemon - ./daemon <port> <introducer port>, introducer port is optional
    // ./Daemon 4560 9870 1230 localhost local/remote introducer
    int introducerNumArgs = 7;
    if(argc>=6){
        bool isIntroducer = argc == introducerNumArgs ? true : false;
        string machineType = isIntroducer ? "introducer":"node";
        string env = argv[5];
        char *daemonPort = argv[1];
        char *hydfsPort = argv[2];
        char *stormPort = argv[3];
        char *introducerName = argv[4];
        cout<<"Starting "<<machineType<<" on "<<daemonPort<<" with introducer on "<<introducerName<<" with "<<env<<" environment\n";

        HyDFS dfs(hydfsPort);
        dfs.start();
        Daemon d(daemonPort, hydfsPort, stormPort);
        d.addListener(&dfs);
        d.start(isIntroducer, introducerName);

        if(string(env) == "remote") {
            dfs.setLocalFilesPath("./files/remote");
        } else {
            if(isIntroducer){
                dfs.setLocalFilesPath("./files/introducer");
            } else {
                dfs.setLocalFilesPath("");
            }
        }

        Manager* managerPtr = NULL;
        if(isIntroducer) {
            managerPtr = new ResourceManager(stormPort, d.getSelfNode(), &dfs);
            d.addListener(managerPtr);
        } else {
            managerPtr = new NodeManager(stormPort, d.getSelfNode(), &dfs, d.getLeaderNode());
            d.addListener(managerPtr);
        }

        managerPtr->start();

        if(isIntroducer) {
            dfs.createFile("TrafficSigns_10000.csv", "TrafficSigns_10000");
            dfs.createFile("TrafficSigns_1000.csv", "TrafficSigns_1000");
            dfs.createFile("TrafficSigns_5000.csv", "TrafficSigns_5000");
            dfs.createFile("rainstormTestFile", "testFile");
        }
        
        while(1){
            string command;
            getline(cin, command);
            if(command == "leave"){
                dfs.stopThreads();
                d.stopThreads();
                managerPtr->stop();
                break;
            }
            else {
                d.runCommand(command);
            }
        }
    } else {
        cout<<"Invalid arguments\n";
    }
    return 0;
}