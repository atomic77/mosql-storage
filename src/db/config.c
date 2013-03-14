#include "config.h"

char* LeaderIP;
int LeaderPort;
long StorageMaxSize;
int StorageMinFreeSize;
int StorageMaxOldVersions;
int MaxPreviousST;
int ValidationBufferSize;
int ValidationDeliverInterval;
int NodeID;
int NumberOfNodes;
int NumberOfCacheNodes;

void set_default_global_variables(void) {
	NodeID = 0;
	StorageMaxOldVersions = 4;
	ValidationBufferSize = 128;
	LeaderIP = "127.0.0.1";
	LeaderPort = 8888;
	StorageMaxSize = 1024*1024*1024;
	StorageMinFreeSize = 1024*1024;
	MaxPreviousST = 128;
	NumberOfNodes = 1;
	NumberOfCacheNodes = 0;
	ValidationDeliverInterval = 4000;
}
