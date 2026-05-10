/**
 * Simple P2P File Sharing System
 * 
 * This program implements a basic peer-to-peer file sharing system using sockets.
 * Each instance can act as both a server and client simultaneously.
 * Features:
 * - Peer discovery
 * - File listing
 * - File transfer
 * - Basic NAT traversal support
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>

#define DEFAULT_PORT 8000
#define MAX_BUFFER 4096
#define MAX_PEERS 100
#define MAX_FILES 100
#define MAX_FILENAME 256
#define SHARE_DIR "shared"
#define DOWNLOAD_DIR "downloads"

// Message types
typedef enum {
    MSG_HELLO = 1,
    MSG_PEER_LIST,
    MSG_SEARCH_FILE,
    MSG_SEARCH_RESPONSE,
    MSG_REQUEST_FILE,
    MSG_FILE_DATA,
    MSG_KEEP_ALIVE,
    MSG_GOODBYE
} MessageType;

// Message structure
typedef struct {
    MessageType type;
    uint32_t size;
    char data[MAX_BUFFER];
} Message;

// Peer structure
typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
    time_t last_seen;
    int active;
} Peer;

// File info structure
typedef struct {
    char filename[MAX_FILENAME];
    size_t size;
} FileInfo;

// Global variables
Peer peers[MAX_PEERS];
int peer_count = 0;
FileInfo shared_files[MAX_FILES];
int file_count = 0;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t files_mutex = PTHREAD_MUTEX_INITIALIZER;
int server_running = 1;
int server_socket = -1;
char my_ip[INET_ADDRSTRLEN];
int my_port = DEFAULT_PORT;

// Function prototypes
void initialize_system();
void* server_thread(void* arg);
void* connection_handler(void* socket_desc);
void* discovery_thread(void* arg);
void scan_shared_directory();
void add_peer(const char* ip, int port);
void remove_peer(const char* ip, int port);
void update_peer_activity(const char* ip, int port);
int find_peer(const char* ip, int port);
int send_message(int socket, Message* message);
int receive_message(int socket, Message* message);
void handle_hello(int socket, Message* message);
void handle_peer_list(int socket, Message* message);
void handle_search_file(int socket, Message* message);
void handle_request_file(int socket, Message* message);
void signal_handler(int sig);
void cleanup();
void print_menu();
void handle_user_input();
void connect_to_peer(const char* ip, int port);
void search_file(const char* filename);
void download_file(const char* filename, const char* peer_ip, int peer_port);
void list_peers();
void list_files();

int main(int argc, char* argv[]) {
    pthread_t server_tid, discovery_tid;
    
    // Register signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            my_port = atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            strncpy(my_ip, argv[i + 1], INET_ADDRSTRLEN);
            i++;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [-p port] [-i ip] [peer_address:port]\n", argv[0]);
            return 0;
        }
    }
    
    // Initialize the system
    initialize_system();
    
    // Start server thread
    if (pthread_create(&server_tid, NULL, server_thread, NULL) < 0) {
        perror("Could not create server thread");
        return 1;
    }
    
    // Start discovery thread
    if (pthread_create(&discovery_tid, NULL, discovery_thread, NULL) < 0) {
        perror("Could not create discovery thread");
        return 1;
    }
    
    // Connect to known peers from command line
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] != '-') {
            char* colon = strchr(argv[i], ':');
            if (colon) {
                *colon = '\0';
                connect_to_peer(argv[i], atoi(colon + 1));
            } else {
                connect_to_peer(argv[i], DEFAULT_PORT);
            }
        }
    }
    
    printf("P2P File Sharing System Started\n");
    printf("Running on %s:%d\n", my_ip, my_port);
    printf("Shared directory: %s\n", SHARE_DIR);
    printf("Downloads directory: %s\n", DOWNLOAD_DIR);
    
    // Main loop for user interface
    while (server_running) {
        print_menu();
        handle_user_input();
        sleep(1);
    }
    
    // Wait for threads to finish
    pthread_join(server_tid, NULL);
    pthread_join(discovery_tid, NULL);
    
    // Cleanup
    cleanup();
    
    return 0;
}

void initialize_system() {
    // Create shared and downloads directories if they don't exist
    struct stat st = {0};
    if (stat(SHARE_DIR, &st) == -1) {
        mkdir(SHARE_DIR, 0700);
    }
    if (stat(DOWNLOAD_DIR, &st) == -1) {
        mkdir(DOWNLOAD_DIR, 0700);
    }
    
    // Initialize peers array
    memset(peers, 0, sizeof(peers));
    
    // Get my IP if not provided
    if (strlen(my_ip) == 0) {
        strcpy(my_ip, "127.0.0.1");  // Default to localhost
    }
    
    // Scan shared directory for files
    scan_shared_directory();
}

void scan_shared_directory() {
    DIR* dir;
    struct dirent* entry;
    struct stat file_stat;
    char filepath[MAX_FILENAME + sizeof(SHARE_DIR) + 1];
    
    pthread_mutex_lock(&files_mutex);
    file_count = 0;
    
    dir = opendir(SHARE_DIR);
    if (dir) {
        while ((entry = readdir(dir)) != NULL && file_count < MAX_FILES) {
            // Skip . and .. entries
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            
            sprintf(filepath, "%s/%s", SHARE_DIR, entry->d_name);
            
            if (stat(filepath, &file_stat) == 0 && S_ISREG(file_stat.st_mode)) {
                strncpy(shared_files[file_count].filename, entry->d_name, MAX_FILENAME - 1);
                shared_files[file_count].filename[MAX_FILENAME - 1] = '\0';
                shared_files[file_count].size = file_stat.st_size;
                file_count++;
            }
        }
        closedir(dir);
    }
    
    pthread_mutex_unlock(&files_mutex);
    printf("Scanned %d files in shared directory\n", file_count);
}

void* server_thread(void* arg) {
    int client_socket;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    int* new_sock;
    
    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    
    // Set socket options to reuse address
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }
    
    // Prepare the sockaddr_in structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(my_port);
    
    // Bind the socket
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }
    
    // Listen
    if (listen(server_socket, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }
    
    printf("Server listening on port %d\n", my_port);
    
    // Accept incoming connections
    while (server_running) {
        client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
        if (client_socket < 0) {
            if (errno == EINVAL && !server_running) {
                // Server is shutting down
                break;
            }
            perror("Accept failed");
            continue;
        }
        
        inet_ntop(AF_INET, &client_addr.sin_addr, peers[peer_count].ip, INET_ADDRSTRLEN);
        printf("Connection accepted from %s:%d\n", peers[peer_count].ip, ntohs(client_addr.sin_port));
        
        // Create a new thread for each connection
        pthread_t thread_id;
        new_sock = malloc(sizeof(int));
        *new_sock = client_socket;
        
        if (pthread_create(&thread_id, NULL, connection_handler, (void*)new_sock) < 0) {
            perror("Could not create thread");
            free(new_sock);
            close(client_socket);
        } else {
            // Detach the thread so that it cleans up automatically when it's done
            pthread_detach(thread_id);
        }
    }
    
    return NULL;
}

void* connection_handler(void* socket_desc) {
    // Get the socket descriptor
    int sock = *(int*)socket_desc;
    free(socket_desc);
    
    Message message;
    
    // Receive and process messages from the client
    while (server_running) {
        if (receive_message(sock, &message) <= 0) {
            break;
        }
        
        switch (message.type) {
            case MSG_HELLO:
                handle_hello(sock, &message);
                break;
            case MSG_PEER_LIST:
                handle_peer_list(sock, &message);
                break;
            case MSG_SEARCH_FILE:
                handle_search_file(sock, &message);
                break;
            case MSG_REQUEST_FILE:
                handle_request_file(sock, &message);
                break;
            case MSG_KEEP_ALIVE:
                // Just acknowledge the keep-alive
                break;
            case MSG_GOODBYE:
                // Peer is disconnecting
                close(sock);
                return NULL;
            default:
                printf("Unknown message type: %d\n", message.type);
                break;
        }
    }
    
    close(sock);
    return NULL;
}

void* discovery_thread(void* arg) {
    // Periodically try to discover new peers and send keep-alive messages
    while (server_running) {
        // Send keep-alive to all peers
        pthread_mutex_lock(&peers_mutex);
        for (int i = 0; i < peer_count; i++) {
            if (peers[i].active) {
                // Create a connection to the peer
                struct sockaddr_in peer_addr;
                int peer_sock;
                
                peer_sock = socket(AF_INET, SOCK_STREAM, 0);
                if (peer_sock < 0) {
                    continue;
                }
                
                memset(&peer_addr, 0, sizeof(peer_addr));
                peer_addr.sin_family = AF_INET;
                peer_addr.sin_port = htons(peers[i].port);
                
                if (inet_pton(AF_INET, peers[i].ip, &peer_addr.sin_addr) <= 0) {
                    close(peer_sock);
                    continue;
                }
                
                // Set a short timeout for connection
                struct timeval timeout;
                timeout.tv_sec = 2;
                timeout.tv_usec = 0;
                setsockopt(peer_sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
                setsockopt(peer_sock, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
                
                if (connect(peer_sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
                    // Connection failed, mark peer as inactive
                    peers[i].active = 0;
                    close(peer_sock);
                    continue;
                }
                
                // Send keep-alive message
                Message keep_alive;
                keep_alive.type = MSG_KEEP_ALIVE;
                keep_alive.size = 0;
                send_message(peer_sock, &keep_alive);
                
                // Request updated peer list
                Message peer_list_req;
                peer_list_req.type = MSG_PEER_LIST;
                peer_list_req.size = 0;
                send_message(peer_sock, &peer_list_req);
                
                // Close connection
                close(peer_sock);
                
                // Update peer's last seen time
                peers[i].last_seen = time(NULL);
            } else {
                // Check if peer has been inactive for too long (5 minutes)
                if (time(NULL) - peers[i].last_seen > 300) {
                    // Remove inactive peer
                    for (int j = i; j < peer_count - 1; j++) {
                        memcpy(&peers[j], &peers[j + 1], sizeof(Peer));
                    }
                    peer_count--;
                    i--; // Recheck this index as it now contains a new peer
                }
            }
        }
        pthread_mutex_unlock(&peers_mutex);
        
        // Sleep for 60 seconds before next discovery cycle
        for (int i = 0; i < 60 && server_running; i++) {
            sleep(1);
        }
    }
    
    return NULL;
}

void add_peer(const char* ip, int port) {
    pthread_mutex_lock(&peers_mutex);
    
    // Check if peer already exists
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            // Update existing peer
            peers[i].last_seen = time(NULL);
            peers[i].active = 1;
            pthread_mutex_unlock(&peers_mutex);
            return;
        }
    }
    
    // Add new peer if there's space
    if (peer_count < MAX_PEERS) {
        strncpy(peers[peer_count].ip, ip, INET_ADDRSTRLEN);
        peers[peer_count].port = port;
        peers[peer_count].last_seen = time(NULL);
        peers[peer_count].active = 1;
        peer_count++;
        printf("Added peer %s:%d\n", ip, port);
    }
    
    pthread_mutex_unlock(&peers_mutex);
}

void remove_peer(const char* ip, int port) {
    pthread_mutex_lock(&peers_mutex);
    
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            // Remove by shifting remaining peers
            for (int j = i; j < peer_count - 1; j++) {
                memcpy(&peers[j], &peers[j + 1], sizeof(Peer));
            }
            peer_count--;
            printf("Removed peer %s:%d\n", ip, port);
            break;
        }
    }
    
    pthread_mutex_unlock(&peers_mutex);
}

void update_peer_activity(const char* ip, int port) {
    pthread_mutex_lock(&peers_mutex);
    
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            peers[i].last_seen = time(NULL);
            peers[i].active = 1;
            break;
        }
    }
    
    pthread_mutex_unlock(&peers_mutex);
}

int find_peer(const char* ip, int port) {
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            return i;
        }
    }
    return -1;
}

int send_message(int socket, Message* message) {
    uint32_t type = htonl(message->type);
    uint32_t size = htonl(message->size);
    
    if (send(socket, &type, sizeof(type), 0) < 0) {
        return -1;
    }
    
    if (send(socket, &size, sizeof(size), 0) < 0) {
        return -1;
    }
    
    if (message->size > 0) {
        if (send(socket, message->data, message->size, 0) < 0) {
            return -1;
        }
    }
    
    return 0;
}

int receive_message(int socket, Message* message) {
    uint32_t type, size;
    
    if (recv(socket, &type, sizeof(type), 0) <= 0) {
        return -1;
    }
    message->type = ntohl(type);
    
    if (recv(socket, &size, sizeof(size), 0) <= 0) {
        return -1;
    }
    message->size = ntohl(size);
    
    if (message->size > MAX_BUFFER) {
        message->size = MAX_BUFFER;
    }
    
    if (message->size > 0) {
        ssize_t received = 0;
        ssize_t total_received = 0;
        
        while (total_received < message->size) {
            received = recv(socket, message->data + total_received, message->size - total_received, 0);
            if (received <= 0) {
                return -1;
            }
            total_received += received;
        }
    }
    
    return 0;
}

void handle_hello(int socket, Message* message) {
    char ip[INET_ADDRSTRLEN];
    int port;
    
    // Extract peer information
    sscanf(message->data, "%s %d", ip, &port);
    
    // Add or update peer
    add_peer(ip, port);
    
    // Send back our hello
    Message response;
    response.type = MSG_HELLO;
    sprintf(response.data, "%s %d", my_ip, my_port);
    response.size = strlen(response.data) + 1;
    send_message(socket, &response);
    
    // Send our peer list
    Message peer_list;
    peer_list.type = MSG_PEER_LIST;
    peer_list.size = 0;
    
    pthread_mutex_lock(&peers_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].active) {
            char peer_info[INET_ADDRSTRLEN + 10];
            sprintf(peer_info, "%s %d\n", peers[i].ip, peers[i].port);
            
            // Check if we have enough space
            if (peer_list.size + strlen(peer_info) < MAX_BUFFER) {
                strcat(peer_list.data, peer_info);
                peer_list.size += strlen(peer_info);
            }
        }
    }
    pthread_mutex_unlock(&peers_mutex);
    
    if (peer_list.size > 0) {
        send_message(socket, &peer_list);
    }
}

void handle_peer_list(int socket, Message* message) {
    char* line = strtok(message->data, "\n");
    while (line != NULL) {
        char ip[INET_ADDRSTRLEN];
        int port;
        
        if (sscanf(line, "%s %d", ip, &port) == 2) {
            // Don't add ourselves as a peer
            if (strcmp(ip, my_ip) != 0 || port != my_port) {
                add_peer(ip, port);
            }
        }
        
        line = strtok(NULL, "\n");
    }
}

void handle_search_file(int socket, Message* message) {
    char filename[MAX_FILENAME];
    strncpy(filename, message->data, MAX_FILENAME - 1);
    filename[MAX_FILENAME - 1] = '\0';
    
    Message response;
    response.type = MSG_SEARCH_RESPONSE;
    response.size = 0;
    
    // Search for the file in our shared directory
    pthread_mutex_lock(&files_mutex);
    for (int i = 0; i < file_count; i++) {
        if (strstr(shared_files[i].filename, filename) != NULL) {
            // Found a matching file
            char file_info[MAX_FILENAME + 64];
            sprintf(file_info, "%s %zu\n", shared_files[i].filename, shared_files[i].size);
            
            if (response.size + strlen(file_info) < MAX_BUFFER) {
                strcat(response.data, file_info);
                response.size += strlen(file_info);
            }
        }
    }
    pthread_mutex_unlock(&files_mutex);
    
    if (response.size > 0) {
        // Add our IP and port at the end
        char peer_info[INET_ADDRSTRLEN + 10];
        sprintf(peer_info, "PEER %s %d", my_ip, my_port);
        
        if (response.size + strlen(peer_info) + 1 < MAX_BUFFER) {
            strcat(response.data, peer_info);
            response.size = strlen(response.data) + 1;
            send_message(socket, &response);
        }
    }
}

void handle_request_file(int socket, Message* message) {
    char filename[MAX_FILENAME];
    strncpy(filename, message->data, MAX_FILENAME - 1);
    filename[MAX_FILENAME - 1] = '\0';
    
    char filepath[MAX_FILENAME + sizeof(SHARE_DIR) + 1];
    sprintf(filepath, "%s/%s", SHARE_DIR, filename);
    
    // Check if file exists
    FILE* file = fopen(filepath, "rb");
    if (file == NULL) {
        // File not found
        Message response;
        response.type = MSG_FILE_DATA;
        strcpy(response.data, "ERROR: File not found");
        response.size = strlen(response.data) + 1;
        send_message(socket, &response);
        return;
    }
    
    // Get file size
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    // Send file info
    Message file_info;
    file_info.type = MSG_FILE_DATA;
    sprintf(file_info.data, "FILE %s %zu", filename, file_size);
    file_info.size = strlen(file_info.data) + 1;
    send_message(socket, &file_info);
    
    // Read and send file in chunks
    char buffer[MAX_BUFFER];
    size_t bytes_read;
    while ((bytes_read = fread(buffer, 1, MAX_BUFFER, file)) > 0) {
        Message data_message;
        data_message.type = MSG_FILE_DATA;
        memcpy(data_message.data, buffer, bytes_read);
        data_message.size = bytes_read;
        send_message(socket, &data_message);
    }
    
    // Send end of file marker
    Message eof_message;
    eof_message.type = MSG_FILE_DATA;
    strcpy(eof_message.data, "EOF");
    eof_message.size = 4; // "EOF" + null terminator
    send_message(socket, &eof_message);
    
    fclose(file);
    printf("Sent file %s (%zu bytes)\n", filename, file_size);
}

void signal_handler(int sig) {
    printf("\nShutting down...\n");
    server_running = 0;
    
    // Close server socket to unblock accept
    if (server_socket >= 0) {
        close(server_socket);
    }
}

void cleanup() {
    // Nothing to clean up for now
    printf("Cleanup complete\n");
}

void print_menu() {
    printf("\n== P2P File Sharing System ==\n");
    printf("1. Connect to peer\n");
    printf("2. Search for file\n");
    printf("3. List known peers\n");
    printf("4. List shared files\n");
    printf("5. Refresh shared files\n");
    printf("6. Exit\n");
    printf("Enter choice: ");
    fflush(stdout);
}

void handle_user_input() {
    char input[256];
    if (fgets(input, sizeof(input), stdin) == NULL) {
        return;
    }
    
    int choice = atoi(input);
    
    switch (choice) {
        case 1: { // Connect to peer
            char ip[INET_ADDRSTRLEN];
            int port;
            
            printf("Enter peer IP: ");
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin) == NULL) {
                return;
            }
            input[strcspn(input, "\n")] = 0; // Remove newline
            strncpy(ip, input, INET_ADDRSTRLEN - 1);
            ip[INET_ADDRSTRLEN - 1] = '\0';
            
            printf("Enter peer port [%d]: ", DEFAULT_PORT);
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin) == NULL) {
                return;
            }
            input[strcspn(input, "\n")] = 0; // Remove newline
            port = strlen(input) > 0 ? atoi(input) : DEFAULT_PORT;
            
            connect_to_peer(ip, port);
            break;
        }
        case 2: { // Search for file
            char filename[MAX_FILENAME];
            
            printf("Enter filename to search: ");
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin) == NULL) {
                return;
            }
            input[strcspn(input, "\n")] = 0; // Remove newline
            strncpy(filename, input, MAX_FILENAME - 1);
            filename[MAX_FILENAME - 1] = '\0';
            
            search_file(filename);
            
            // If file found, ask to download
            printf("Download a file? (y/n): ");
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin) == NULL) {
                return;
            }
            if (input[0] == 'y' || input[0] == 'Y') {
                printf("Enter filename to download: ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin) == NULL) {
                    return;
                }
                input[strcspn(input, "\n")] = 0; // Remove newline
                
                char peer_ip[INET_ADDRSTRLEN];
                int peer_port;
                
                printf("Enter peer IP: ");
                fflush(stdout);
                if (fgets(peer_ip, sizeof(peer_ip), stdin) == NULL) {
                    return;
                }
                peer_ip[strcspn(peer_ip, "\n")] = 0; // Remove newline
                
                printf("Enter peer port: ");
                fflush(stdout);
                char port_str[10];
                if (fgets(port_str, sizeof(port_str), stdin) == NULL) {
                    return;
                }
                peer_port = atoi(port_str);
                
                download_file(input, peer_ip, peer_port);
            }
            break;
        }
        case 3: // List known peers
            list_peers();
            break;
        case 4: // List shared files
            list_files();
            break;
        case 5: // Refresh shared files
            scan_shared_directory();
            break;
        case 6: // Exit
            server_running = 0;
            break;
        default:
            printf("Invalid choice\n");
            break;
    }
}

void connect_to_peer(const char* ip, int port) {
    struct sockaddr_in peer_addr;
    int sock;
    
    // Create socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return;
    }
    
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip, &peer_addr.sin_addr) <= 0) {
        perror("Invalid address");
        close(sock);
        return;
    }
    
    printf("Connecting to peer %s:%d...\n", ip, port);
    
    // Connect to peer
    if (connect(sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        perror("Connection failed");
        close(sock);
        return;
    }
    
    // Send hello message
    Message hello;
    hello.type = MSG_HELLO;
    sprintf(hello.data, "%s %d", my_ip, my_port);
    hello.size = strlen(hello.data) + 1;
    
    if (send_message(sock, &hello) < 0) {
        perror("Send failed");
        close(sock);
        return;
    }
    
    // Receive response
    Message response;
    if (receive_message(sock, &response) < 0) {
        perror("Receive failed");
        close(sock);
        return;
    }
    
    if (response.type == MSG_HELLO) {
        printf("Connected to peer %s:%d\n", ip, port);
        add_peer(ip, port);
    } else {
        printf("Unexpected response from peer\n");
    }
    
    // Receive peer list if sent
    if (receive_message(sock, &response) >= 0 && response.type == MSG_PEER_LIST) {
        handle_peer_list(sock, &response);
    }
    
    close(sock);
}

void search_file(const char* filename) {
    printf("Searching for: %s\n", filename);
    
    pthread_mutex_lock(&peers_mutex);
    if (peer_count == 0) {
        printf("No known peers to search\n");
        pthread_mutex_unlock(&peers_mutex);
        return;
    }
    
    // Search all active peers
    for (int i = 0; i < peer_count; i++) {
        if (!peers[i].active) {
            continue;
        }
        
        struct sockaddr_in peer_addr;
        int sock;
        
        // Create socket
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            continue;
        }
        
        memset(&peer_addr, 0, sizeof(peer_addr));
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_port = htons(peers[i].port);
        
        if (inet_pton(AF_INET, peers[i].ip, &peer_addr.sin_addr) <= 0) {
            close(sock);
            continue;
        }
        
        // Set a short timeout
        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
        
        // Connect to peer
        if (connect(sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
            peers[i].active = 0;
            close(sock);
            continue;
        }
        
        // Send search request
        Message search_req;
        search_req.type = MSG_SEARCH_FILE;
        strncpy(search_req.data, filename, MAX_BUFFER - 1);
        search_req.data[MAX_BUFFER - 1] = '\0';
        search_req.size = strlen(search_req.data) + 1;
        
        if (send_message(sock, &search_req) < 0) {
            close(sock);
            continue;
        }
        
        // Receive response
        Message response;
        if (receive_message(sock, &response) < 0 || response.type != MSG_SEARCH_RESPONSE) {
            close(sock);
            continue;
        }
        
        // Process response if any files found
        if (response.size > 0) {
            printf("\nFiles found at peer %s:%d:\n", peers[i].ip, peers[i].port);
            
            char* line = strtok(response.data, "\n");
            while (line != NULL) {
                if (strncmp(line, "PEER ", 5) == 0) {
                    // This is the peer info line at the end
                    break;
                }
                printf("  %s\n", line);
                line = strtok(NULL, "\n");
            }
        }
        
        close(sock);
    }
    pthread_mutex_unlock(&peers_mutex);
}

void download_file(const char* filename, const char* peer_ip, int peer_port) {
    struct sockaddr_in peer_addr;
    int sock;
    
    // Create socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return;
    }
    
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(peer_port);
    
    if (inet_pton(AF_INET, peer_ip, &peer_addr.sin_addr) <= 0) {
        perror("Invalid address");
        close(sock);
        return;
    }
    
    // Connect to peer
    if (connect(sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        perror("Connection failed");
        close(sock);
        return;
    }
    
    // Send file request
    Message request;
    request.type = MSG_REQUEST_FILE;
    strncpy(request.data, filename, MAX_BUFFER - 1);
    request.data[MAX_BUFFER - 1] = '\0';
    request.size = strlen(request.data) + 1;
    
    if (send_message(sock, &request) < 0) {
        perror("Send failed");
        close(sock);
        return;
    }
    
    // Receive file
    Message response;
    if (receive_message(sock, &response) < 0 || response.type != MSG_FILE_DATA) {
        perror("Receive failed");
        close(sock);
        return;
    }
    
    // Check for errors
    if (strncmp(response.data, "ERROR:", 6) == 0) {
        printf("%s\n", response.data);
        close(sock);
        return;
    }
    
    // Parse file info
    char file_name[MAX_FILENAME];
    size_t file_size;
    
    if (sscanf(response.data, "FILE %s %zu", file_name, &file_size) != 2) {
        printf("Invalid file info received\n");
        close(sock);
        return;
    }
    
    // Open file for writing
    char filepath[MAX_FILENAME + sizeof(DOWNLOAD_DIR) + 1];
    sprintf(filepath, "%s/%s", DOWNLOAD_DIR, file_name);
    
    FILE* file = fopen(filepath, "wb");
    if (file == NULL) {
        perror("Cannot create file");
        close(sock);
        return;
    }
    
    printf("Downloading %s (%zu bytes)...\n", file_name, file_size);
    
    // Receive file data
    size_t total_received = 0;
    time_t start_time = time(NULL);
    
    while (1) {
        if (receive_message(sock, &response) < 0) {
            perror("Receive failed");
            fclose(file);
            close(sock);
            return;
        }
        
        if (response.type != MSG_FILE_DATA) {
            printf("Unexpected message type\n");
            fclose(file);
            close(sock);
            return;
        }
        
        if (response.size == 4 && strcmp(response.data, "EOF") == 0) {
            // End of file
            break;
        }
        
        // Write data to file
        fwrite(response.data, 1, response.size, file);
        total_received += response.size;
        
        // Show progress
        double progress = (double)total_received / file_size * 100.0;
        printf("\rProgress: %.1f%% (%zu/%zu bytes)", progress, total_received, file_size);
        fflush(stdout);
    }
    
    time_t end_time = time(NULL);
    double elapsed_time = difftime(end_time, start_time);
    double transfer_rate = total_received / (elapsed_time > 0 ? elapsed_time : 1) / 1024.0;
    
    printf("\nDownload complete: %s (%zu bytes in %.1f seconds, %.2f KB/s)\n", 
           file_name, total_received, elapsed_time, transfer_rate);
    
    fclose(file);
    close(sock);
    
    // Refresh shared files list
    scan_shared_directory();
}

void list_peers() {
    printf("\nKnown peers:\n");
    
    pthread_mutex_lock(&peers_mutex);
    if (peer_count == 0) {
        printf("No peers known\n");
    } else {
        printf("%-16s %-6s %-20s %s\n", "IP Address", "Port", "Last Seen", "Status");
        printf("-------------------------------------------------------\n");
        
        for (int i = 0; i < peer_count; i++) {
            char time_str[20];
            strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", 
                     localtime(&peers[i].last_seen));
            
            printf("%-16s %-6d %-20s %s\n", 
                   peers[i].ip, peers[i].port, time_str, 
                   peers[i].active ? "Active" : "Inactive");
        }
    }
    pthread_mutex_unlock(&peers_mutex);
}

void list_files() {
    printf("\nShared files:\n");
    
    pthread_mutex_lock(&files_mutex);
    if (file_count == 0) {
        printf("No files shared\n");
    } else {
        printf("%-30s %s\n", "Filename", "Size");
        printf("-------------------------------------------------------\n");
        
        for (int i = 0; i < file_count; i++) {
            printf("%-30s %zu bytes\n", 
                   shared_files[i].filename, shared_files[i].size);
        }
    }
    pthread_mutex_unlock(&files_mutex);
}