## CS425 MP2 - Distributed Failure Detector

  

Follow these instructions to run the distributed Failure Detector -

  

1. Configure introducer host name and port name in Daemon.cpp file
2. Build the main.cpp program on all machines on your distributed system using
	`make`
3. Daemon executables would be build with g++ compiler
4. Run the introducer on one of the machine using pre configured port number using
    `.Daemon <port number> introducer`
5. Run the daemon on all other machines using following command
	`./server <port number>`
6. Use commands given in the `Daemon.cpp`'s `start()` method to change the behavior or get info about the membership list on that daemon.
7. Change the conatant timeouts and messsage drop probability defined on the top of `Daemon.cpp` file to run experiments.



Alternatively, to build daemon on all machines run `bash init.sh`. Make sure to follow instruction and change paths to secret tokens and passwords in the script. You can also configure port number for server in the file.
