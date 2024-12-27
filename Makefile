CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17
# Target and source files
TARGET = Daemon
SRCS = main.cpp utils.cpp HyDFSMessage.cpp Node.cpp FileMetaData.cpp HyDFS.cpp ConsistentHashRing.cpp ResourceManager.cpp NodeManager.cpp Operations.cpp Manager.cpp RainStormMessage.cpp Task/Task.cpp Task/SourceTask.cpp Task/SplitTask.cpp Task/CountTask.cpp Task/FilterTask.cpp Task/SelectTask.cpp Task/FilterSignPostTask.cpp Task/CountCategoryTask.cpp
OBJS = $(SRCS:.cpp=.o)

# Rules
all: cfiles $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)
	rm -f $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET) ./stats/*.txt

cfiles:
	rm -rf ./files/A* ./files/wireless* ./files/introducer/hydfs/* ./files/remote/hydfs/* ./files/fa24-cs425*