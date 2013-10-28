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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "dsmDB_priv.h"
#include "peer.h"


static node_info* node_info_table = NULL;
static node_info* cache_node_info_table = NULL;


int starts_with(char * match, char * buffer) {
    int size = strlen(match);
    return strncmp(match, buffer, size);
}

node_info * get_node_info(int node_id) {
    if(node_id < 0) {
        printf("Invalid node info requested: %d, (%d Nodes)\n", node_id, NumberOfNodes);
        exit(1);
    }
    
    if (node_id >= NumberOfNodes) {
        return &cache_node_info_table[node_id - NumberOfNodes];
	} else {
		return &node_info_table[node_id];
	}
}

//Check that all values were configured
void check_values(int node_count) {

/*    if(NumberOfNodes != node_count) {
        printf("Error: Some node configurations are missing\n");
        exit(1);
    }
 */   
    if(ValidationBufferSize == -1) {
        printf("Error: ValidationBufferSize not initialized\n");
        exit(1);
    }

    if(LeaderIP == NULL) {
        printf("Error: LeaderIP not initialized\n");
        exit(1);
    }

    if(LeaderPort == -1) {
        printf("Error: LeaderPort not initialized\n");
        exit(1);
    }
    
    if(StorageMaxSize == -1) {
        printf("Error: StorageMaxSize not initialized\n");
        exit(1);
    }
    
    if(StorageMinFreeSize == -1) {
        printf("Error: StorageMinFreeSize not initialized\n");
        exit(1);
    }

    if(StorageMaxOldVersions == -1) {
        printf("Error: StorageMaxOldVersions not initialized\n");
        exit(1);
    }

    if(MaxPreviousST == -1) {
        printf("Error: MaxPreviousST not initialized\n");
        exit(1);
    }
/*    
    if(NumberOfNodes == -1) {
        printf("Error: NumberOfNodes not initialized\n");
        exit(1);
    }
    */
}

int load_config_file(const char * path) {
	FILE* fp;
	printf("Opening:%s\n", path);
	if ((fp = fopen(path,"r")) == NULL) {
		perror("fopen");
		exit(1);
	}
	
	char string[256];
	char tmp[256];
    int tmp_id;
    char tmp_ip[32];
    int tmp_port;
    int size;
    
    int node_count = 0;
    int cache_node_count = 0;

	while(fgets(string, 256, fp) != NULL) {
		
		if(starts_with("ValidationBufferSize", string) == 0) {		    
            sscanf(string, "%s %d", tmp, &ValidationBufferSize);
            printf("Setting ValidationBufferSize: %d\n", ValidationBufferSize);
            continue;
        }

		if(starts_with("LeaderIP", string) == 0) {
            char tmp_leader_ip[32];
            sscanf(string, "%s %s", tmp, tmp_leader_ip);
            LeaderIP = malloc(strlen(tmp_leader_ip));
            memcpy(LeaderIP, tmp_leader_ip, strlen(tmp_leader_ip));
            printf("Setting LeaderIP: %s\n", LeaderIP);
            continue;
        }

		if(starts_with("LeaderPort", string) == 0) {		    
            sscanf(string, "%s %d", tmp, &LeaderPort);
            printf("Setting LeaderPort: %d\n", LeaderPort);
            continue;
        }
        
        if(starts_with("StorageMaxSize", string) == 0) {           
            sscanf(string, "%s %ld", tmp, &StorageMaxSize);
            printf("Setting StorageMaxSize: %ld\n", StorageMaxSize);
            continue;
        }
        
        if(starts_with("StorageMinFreeSize", string) == 0) {           
            sscanf(string, "%s %d", tmp, &StorageMinFreeSize);
            printf("Setting StorageMinFreeSize: %d\n", StorageMinFreeSize);
            continue;
        }

		if(starts_with("StorageMaxOldVersions", string) == 0) {		    
            sscanf(string, "%s %d", tmp, &StorageMaxOldVersions);
            printf("Setting StorageMaxOldVersions: %d\n", StorageMaxOldVersions);
            continue;
        }
        
        if(starts_with("MaxPreviousST", string) == 0) {		    
            sscanf(string, "%s %d", tmp, &MaxPreviousST);
            printf("Setting MaxPreviousST: %d\n", MaxPreviousST);
            continue;
        }

 /*       if(starts_with("NumberOfNodes", string) == 0) {		    
            sscanf(string, "%s %d", tmp, &NumberOfNodes);
            printf("Setting NumberOfNodes: %d\n", NumberOfNodes);
            node_info_table = calloc(NumberOfNodes, sizeof(node_info));
            continue;
        }
        
        if(starts_with("NumberOfCacheNodes", string) == 0) {
            sscanf(string, "%s %d", tmp, &NumberOfCacheNodes);
            printf("Setting NumberOfCacheNodes: %d\n", NumberOfCacheNodes);
            cache_node_info_table = calloc(NumberOfCacheNodes, sizeof(node_info));
            continue;
        }
        */

		if (starts_with("ValidationDeliverInterval", string) == 0) {
			sscanf(string, "%s %d", tmp, &ValidationDeliverInterval);
			printf("Setting ValidationDeliverInterval: %d\n", ValidationDeliverInterval);
			continue;
		}
/*        
        if(starts_with("node", string) == 0) {
            node_info * n;
            if(node_info_table == NULL) {
                printf("Config error: NumberOfNodes must appear before individual node info\n");
                exit(1);
            }
            
            if(node_count == NumberOfNodes) {
                printf("Config error: More node infos than NumberOfNodes\n");
                exit(1);
                
            }
            
            sscanf(string, "%s %d %s %d", tmp, &tmp_id, tmp_ip, &tmp_port);            
            if(tmp_id != node_count) {
                printf("Config error: Individual node info must be in order, expected %d found %d\n", node_count, tmp_id);
                exit(1);
            }

            n = &node_info_table[node_count];
            n->port = tmp_port;
            size = strlen(tmp_ip);
            
            if(size > 16) {
                printf("Config error: Node %d, invalid IP\n", node_count);
                exit(1);
            }
            memcpy(n->ip, tmp_ip, size);
            n->ip[size] = '\0';                   
            printf("Setting Node %d: %s:%d\n", node_count, n->ip, n->port);
			peer_add(node_count, n->ip, tmp_port);
			
            node_count++;
			
            continue;
        }
        
        
        if(starts_with("cachenode", string) == 0) {
            node_info * n;
            if(cache_node_info_table == NULL) {
                printf("Config error: NumberOfCacheNodes must appear before individual node info\n");
                exit(1);
            }
            
            if(cache_node_count == NumberOfCacheNodes) {
                printf("Config error: More node infos than NumberOfCacheNodes\n");
                exit(1);
            }
            
            sscanf(string, "%s %d %s %d", tmp, &tmp_id, tmp_ip, &tmp_port);            
            if(tmp_id != cache_node_count + NumberOfNodes) {
                printf("Config error: Individual node info must be in order, expected %d found %d\n", cache_node_count, tmp_id);
                exit(1);
            }

            n = &cache_node_info_table[cache_node_count];
            n->port = tmp_port;
            size = strlen(tmp_ip);
            
            if(size > 16) {
                printf("Config error: Node %d, invalid IP\n", cache_node_count);
                exit(1);
            }
            
            memcpy(n->ip, tmp_ip, size);
            n->ip[size] = '\0';                   
            printf("Setting Cache Node %d: %s:%d\n", cache_node_count, n->ip, n->port);
			peer_add_cache_node(tmp_id, n->ip, n->port);
			
            cache_node_count++;
            continue;
        }
*/
        if(starts_with("recnode", string) == 0) {
            sscanf(string, "%s %d %s %d", tmp, &tmp_id, tmp_ip, &tmp_port);            
			size = strlen(tmp_ip);
            if(size > 16) {
                printf("Config error: recnode %s, invalid IP\n", tmp_ip);
                exit(1);
            }
            
            printf("Setting Rec Node %d: %s:%d\n", tmp_id, tmp_ip, tmp_port);
			peer_add_recnode(tmp_id, tmp_ip, tmp_port);
			
            continue;
        }
        
        // if(starts_with("XXXX") == 0) {           
        //     sscanf(string, "%s %d", tmp, &XXXX);
        //     printf("Setting XXXX: %d\n", XXXX);
        //     continue;
        // }
            
		if (starts_with("//", string) == 0) {
            // printf("Skipping comment\n");
            continue;
		}
		if (starts_with("\n", string) == 0) {
            // printf("Skipping newline\n");
            continue;
        }
		
        printf("Config error:\n");
        printf("Line: %s", string);
		
	}
	
    check_values(node_count);
	return 0;
}

// int main (int argc, char const *argv[])
// {
//     load_config_file("config.cfg");
//     return 0;
// }
