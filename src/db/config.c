/*
    Copyright (C) 2013 University of Lugano

	This file is part of the MoSQL storage system. 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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
