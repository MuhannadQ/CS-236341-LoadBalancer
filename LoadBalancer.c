#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <assert.h>
#include <pthread.h>

#define LB_ADDRESS "10.0.0.1"
#define LB_PORT 80
#define SERVERS_PORT 80
#define SERVERS_COUNT 3
#define BUFFER_SIZE 256

typedef struct CustomerRequest {
    int client_socket;
    int customer_num;
    char request_type;
    int request_len;
} *CustomerRequest;

typedef struct CyclicBuffer {
    int fifo_read;
    int fifo_write;
    bool fifo_full;
    CustomerRequest fifo[BUFFER_SIZE];

} *CyclicBuffer;

//Each server has a cyclic buffer of requests it sent (so when the request finishes we can decrease its load from the load field//
typedef struct ServerConnection {
    char server_name[10];
    char server_address[15];
    int lb_server_socket;
    int load;
    int delta;
    int new_load;
    CyclicBuffer request_fifo;

} *ServerConnection;

ServerConnection servers_connections[SERVERS_COUNT];

int chooseServer(ServerConnection servers_connections[], char request_type, int request_len);
void initServerConnections(ServerConnection servers_connections[]);
void *clientToServerThread(void *vargp);
void *serverToClientThread(void *vargp);

CustomerRequest InitRequest(int client_socket, int customer_num, char request_type, int request_len) {
    CustomerRequest c = (CustomerRequest)malloc(sizeof(struct CustomerRequest));
    c->client_socket = client_socket;
    c->customer_num = customer_num;
    c->request_type = request_type;
    c->request_len = request_len;
    return c;
}

CustomerRequest Pop(CyclicBuffer cyclic_buffer) {
    if ((cyclic_buffer->fifo_read == cyclic_buffer->fifo_write) && !cyclic_buffer->fifo_full) return NULL;
    CustomerRequest c = cyclic_buffer->fifo[cyclic_buffer->fifo_read] ;
    cyclic_buffer->fifo_read = (cyclic_buffer->fifo_read + 1) % BUFFER_SIZE;
    return c;
}

//Removes a request from the server of number 'server_num'//
CustomerRequest RemoveCustomerRequest(ServerConnection servers_connections[], int server_num) {
    ServerConnection s = servers_connections[server_num];
    int multiplier = 0;
    CustomerRequest c = Pop(s->request_fifo);
    if (c == NULL) {
        return NULL;
    }
    if (server_num == 0 || server_num == 1) {
        if(c->request_type == 'M') multiplier = 2;
        if (c->request_type == 'P') multiplier = 1;
        if (c->request_type == 'V') multiplier = 1;
    }
    else if (server_num == 2) {
        if (c->request_type == 'M') multiplier = 1;
        if (c->request_type == 'P') multiplier = 2;
        if (c->request_type == 'V') multiplier = 3;

    }
    int delta = multiplier * c->request_len;
    s->load -= delta;

    assert(s->load >= 0);
    return c;
}

void Push(CyclicBuffer cyclic_buffer, CustomerRequest c) {
    printf("Push debug 1\n");
    assert(cyclic_buffer->fifo_full == false);
    printf("Push debug 2\n");
    cyclic_buffer->fifo[cyclic_buffer->fifo_write] = c;
    cyclic_buffer->fifo_write = (cyclic_buffer->fifo_write + 1) % BUFFER_SIZE;
    printf("Push debug 3\n");
    if (cyclic_buffer->fifo_read == cyclic_buffer->fifo_write) cyclic_buffer->fifo_full = true;

}

//Adds a request to a server (which will be chosen appropriatly by chooseServer method)//
int AddCustomerRequest(ServerConnection servers_connections[], CustomerRequest c) {
    printf("AddCustomerRequest debug 1\n");
    int server_num = chooseServer(servers_connections, c->request_type, c->request_len);
    ServerConnection s = servers_connections[server_num];
    printf("AddCustomerRequest debug 2\n");
    int multiplier = 0;
    Push(s->request_fifo, c);
    printf("AddCustomerRequest debug 3\n");
    if (server_num == 0 || server_num == 1) {
        if (c->request_type == 'M') multiplier = 2;
        if (c->request_type == 'P') multiplier = 1;
        if (c->request_type == 'V') multiplier = 1;
    }
    else if (server_num == 2) {
        if (c->request_type == 'M') multiplier = 1;
        if (c->request_type == 'P') multiplier = 2;
        if (c->request_type == 'V') multiplier = 3;

    }
    int delta = multiplier * c->request_len;
    s->load += delta;
    printf("%d\n", server_num);
    return server_num;
}

void printServerConnections(ServerConnection servers_connections[]) {
    for (int i = 0; i < SERVERS_COUNT; i++) {
        printf("server_name: %s\n", servers_connections[i]->server_name);
        printf("server_address: %s\n", servers_connections[i]->server_address);
        printf("lb_server_socket: %d\n", servers_connections[i]->lb_server_socket);
        printf("load: %d\n", servers_connections[i]->load);
        printf("delta: %d\n", servers_connections[i]->delta);
        printf("new_load: %d\n", servers_connections[i]->new_load);
        printf("\n");
    }
}

CyclicBuffer InitCyclicBuffer() {
    CyclicBuffer cb = (CyclicBuffer)malloc(sizeof(struct CyclicBuffer));
    cb->fifo_read = 0;
    cb->fifo_write = 0;
    cb->fifo_full = false;
    return cb;
}

int main() {
    pthread_t server_thread_ids[SERVERS_COUNT];
    for (int i = 0; i < SERVERS_COUNT; i++) {
        pthread_create(server_thread_ids + i, NULL, &serverToClientThread, &i);
    }
    CustomerRequest cyclic_buffer[BUFFER_SIZE];
    // ------------------------------- Connect To Servers -------------------------------
    initServerConnections(servers_connections);
    printServerConnections(servers_connections);
    // ------------------------------- Listen To Clients -------------------------------
    int master_socket = socket(AF_INET, SOCK_STREAM, 0);
    /* Create server socket */
    if (master_socket == -1) {
        fprintf(stderr, "Error creating socket --> %s", strerror(errno));
        exit(EXIT_FAILURE);
    }

    /*creating server address*/
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));/* Zeroing server address struct */
    server_addr.sin_family = AF_INET;//filling it
    //inet_pton(AF_INET, LB_ADDRESS, &(server_addr.sin_addr)); // or :
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(LB_PORT);

    /* Bind */
    if ((bind(master_socket, (struct sockaddr*)&server_addr, sizeof(struct sockaddr))) != 0) {
        fprintf(stderr, "Error on bind --> %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    /* Listening to incoming connections allowing 5 connections */
    if ((listen(master_socket, 5)) != 0) {
        fprintf(stderr, "Error on listen --> %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    // ------------------------------- Accept Clients -------------------------------
    socklen_t sock_len = sizeof(struct sockaddr_in);
    struct sockaddr_in client_addr;
    char buffer[2];
    pthread_t client_thread_id = NULL;
    
    while (1) {
        int* server_index;
        printf("Waiting on \'accept\'\n");
        int client_socket = accept(master_socket, (struct sockaddr*)&client_addr, &sock_len);
        if (client_socket == -1) {
            fprintf(stderr, "Error on accept --> %s", strerror(errno));
            exit(EXIT_FAILURE);
        }
        char* client_ip_address = inet_ntoa(client_addr.sin_addr);
        fprintf(stdout, "Accept peer --> %s\n", client_ip_address);

        pthread_create(&client_thread_id, NULL, &clientToServerThread, &client_socket);
        pthread_join(client_thread_id, (void**) &server_index);



        // char buffer[2];
        // // memset(buffer, 0, sizeof(buffer));
        // int data_len = recv(servers_connections[*server_index]->lb_server_socket, buffer, sizeof(buffer), 0);
        // if (data_len < 0) {
        //     fprintf(stderr, "Error on receiving result --> %s", strerror(errno));
        //     exit(EXIT_FAILURE);
        // }
        // printf("received data_len from server: %d\n", data_len);
        // printf("received buffer from server: %c%c\n", buffer[0], buffer[1]);
        
        // send(client_socket, buffer, sizeof(buffer), 0);
        // RemoveCustomerRequest(servers_connections, *server_index);
        // close(client_socket);



    }
    // pthread_join(server_thread_id_1, NULL);
    // pthread_join(server_thread_id_2, NULL);
    // pthread_join(server_thread_id_3, NULL);
    // close(master_socket);
    // return 0;
}

int chooseServer(ServerConnection servers_connections[], char request_type, int request_len) {
    int delta = 0;
    if (request_type == 'M') {
        servers_connections[0]->delta = 2 * (request_len);
        servers_connections[1]->delta = 2 * (request_len);
        servers_connections[2]->delta = 1 * (request_len);
    }
    if (request_type == 'V') {
        servers_connections[0]->delta = 1 * (request_len);
        servers_connections[1]->delta = 1 * (request_len);
        servers_connections[2]->delta = 3 * (request_len);
    }
    if (request_type == 'P') {
        servers_connections[0]->delta = 1 * (request_len);
        servers_connections[1]->delta = 1 * (request_len);
        servers_connections[2]->delta = 2 * (request_len);
    }

    int server_index = 0;
    int min_load = INT_MAX;
    int min_delta = INT_MAX;
    for (int i = 0; i < SERVERS_COUNT; ++i) {
        servers_connections[i]->new_load = servers_connections[i]->load + servers_connections[i]->delta;
        if (servers_connections[i]->new_load < min_load) {
            min_load = servers_connections[i]->new_load;
            min_delta = servers_connections[i]->delta;
            server_index = i;
        } else if ((servers_connections[i]->new_load == min_load) && (servers_connections[i]->delta < min_delta)) {
            min_delta = servers_connections[i]->delta;
            server_index = i;
        }
    
    }

    servers_connections[server_index]->load += servers_connections[server_index]->delta;
    return server_index;
}












int createLBServerSocket(char* server_address) {
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(server_address);
    //inet_pton(AF_INET, server_address, &(server_addr.sin_addr));
    server_addr.sin_port = htons(SERVERS_PORT);
    /* Create client socket */
    int lb_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (lb_server_socket == -1) {
        fprintf(stderr, "Error creating socket --> %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    /* Connect to the server */
    if (connect(lb_server_socket, (struct sockaddr *)&server_addr, sizeof(struct sockaddr)) != 0) {
        fprintf(stderr, "Error on connect --> %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    } else {
        printf("Connected to the server %s\n", server_address);
    }
    return lb_server_socket;
}

void initServerConnections(ServerConnection servers_connections[]) {
    for (int i = 0; i < SERVERS_COUNT; i++) {
        servers_connections[i] = (ServerConnection)malloc(sizeof(struct ServerConnection));
        char servNumber = (char)i + '1';
        char server_name[] = "serv$";
        server_name[4] = servNumber;
        char server_address[] = "192.168.0.10$";
        server_address[12] = servNumber;
        strcpy(servers_connections[i]->server_name, server_name);
        strcpy(servers_connections[i]->server_address, server_address);
        servers_connections[i]->load = 0;
        servers_connections[i]->delta = 0;
        servers_connections[i]->new_load = 0;
        servers_connections[i]->request_fifo = InitCyclicBuffer();
        servers_connections[i]->lb_server_socket = createLBServerSocket(server_address);
    }
}

void *clientToServerThread(void *vargp) {
    int client_socket = *((int *) vargp);
    printf("in clientToServerThread, client_socket: %d\n", client_socket);
    
    char buffer[2];
    // memset(buffer, 0, sizeof(buffer));
    int data_len = recv(client_socket, buffer, sizeof(buffer), 0);
    if (data_len < 0) {
        fprintf(stderr, "Error on receiving command --> %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    printf("received data_len from client: %d\n", data_len);
    printf("received buffer from client: %c%c\n", buffer[0], buffer[1]);

    CustomerRequest customer_req = InitRequest(client_socket, 0, buffer[0], buffer[1]);
    // continue building customer_req
    printf("debug 1\n");
    int server_index = AddCustomerRequest(servers_connections, customer_req);
    printf("debug 2\n");
    ServerConnection server_conn = servers_connections[server_index];

    send(server_conn->lb_server_socket, buffer, sizeof(buffer), 0);
    printf("debug 3\n");
    int* result = malloc(sizeof(int));
    *result = server_index;
    return (void *) result;
}

void *serverToClientThread(void *vargp) {
    int server_index = *((int *) vargp);
    while (1) {
        printf("in serverToClientThread, server_index: %d\n", server_index);
        ServerConnection server_conn = servers_connections[server_index];
        CustomerRequest customer_req = RemoveCustomerRequest(servers_connections, server_index);
        if (customer_req == NULL) {
            usleep(100000);
            continue;
        }
        char buffer[2];
        // memset(buffer, 0, sizeof(buffer));
        int data_len = recv(server_conn->lb_server_socket, buffer, sizeof(buffer), 0);
        if (data_len < 0) {
            fprintf(stderr, "Error on receiving result --> %s", strerror(errno));
            exit(EXIT_FAILURE);
        }
        printf("received data_len from server: %d\n", data_len);
        printf("received buffer from server: %c%c\n", buffer[0], buffer[1]);
        
        send(customer_req->client_socket, buffer, sizeof(buffer), 0);
        close(customer_req->client_socket);
    }
    pthread_exit(0);
}