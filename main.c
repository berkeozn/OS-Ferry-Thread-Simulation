#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <semaphore.h>

// Ayarlar (ayarlar.txt den okunacak)
int NUM_CARS, NUM_MINIBUSES, NUM_TRUCKS, FERRY_CAPACITY, TOTAL_VEHICLES;

// Istatistikler
time_t sim_start;
double total_wait_time = 0;
double max_wait_time   = 0;
int    total_ferry_trips  = 0;
int    total_loaded_units = 0;
pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int  id;
    char type[20];
    int  size;
    char start_side;
    char current_side;
    time_t arrival_time;
    int  ticket_no;
} Vehicle;

// Gise Semaforlari — proper blocking, busy-waiting yok
sem_t sem_toll_A, sem_toll_B;

// Feribot ve FIFO sistemi
pthread_mutex_t dock_mutex    = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  cond_board_A  = PTHREAD_COND_INITIALIZER;
pthread_cond_t  cond_board_B  = PTHREAD_COND_INITIALIZER;
pthread_cond_t  cond_unload   = PTHREAD_COND_INITIALIZER;

int ferry_load        = 0;
char ferry_side;
int  is_boarding      = 0;
volatile int completed_vehicles = 0;   // FIX 3: volatile — ferry okurken lock almaya gerek kalmaz

int ticket_counter_A = 0, serving_ticket_A = 0;
int ticket_counter_B = 0, serving_ticket_B = 0;

// Zaman damgasi
long get_sim_time() { return (long)(time(NULL) - sim_start); }

void pass_toll_and_board(Vehicle *v) {
    // --- 1. GISE (SEMAFOR ile — tam blocking, no busy-wait) ---
    printf("[Time %ld] %s-%d entered toll on Side %c\n",
           get_sim_time(), v->type, v->id, v->current_side);

    if (v->current_side == 'A') sem_wait(&sem_toll_A);
    else                         sem_wait(&sem_toll_B);

    usleep((rand() % 200) * 1000);   // gise islemi

    printf("[Time %ld] %s-%d passed toll on Side %c\n",
           get_sim_time(), v->type, v->id, v->current_side);

    if (v->current_side == 'A') sem_post(&sem_toll_A);
    else                         sem_post(&sem_toll_B);

    // --- 2. ISKELE — FIFO bileti al ---
    pthread_mutex_lock(&dock_mutex);
    int my_ticket = (v->current_side == 'A') ? ticket_counter_A++
                                              : ticket_counter_B++;
    v->arrival_time = time(NULL);

    printf("[Time %ld] %s-%d joined ferry queue (Ticket: %d) on Side %c\n",
           get_sim_time(), v->type, v->id, my_ticket, v->current_side);

    // FIFO KURALI: sira bende mi + gemi burada mi + boarding acik mi + yer var mi?
    while (ferry_side != v->current_side
        || is_boarding == 0
        || (v->current_side == 'A' ? my_ticket != serving_ticket_A
                                   : my_ticket != serving_ticket_B)
        || (ferry_load + v->size) > FERRY_CAPACITY)
    {
        if (v->current_side == 'A') pthread_cond_wait(&cond_board_A, &dock_mutex);
        else                         pthread_cond_wait(&cond_board_B, &dock_mutex);
    }

    // --- Gemiye binis ---
    double wait = difftime(time(NULL), v->arrival_time);
    pthread_mutex_lock(&stats_mutex);
    total_wait_time += wait;
    if (wait > max_wait_time) max_wait_time = wait;
    pthread_mutex_unlock(&stats_mutex);

    ferry_load += v->size;
    if (v->current_side == 'A') serving_ticket_A++;
    else                         serving_ticket_B++;

    printf("[Time %ld] Ferry loaded %s-%d (Load: %d/%d)\n",
           get_sim_time(), v->type, v->id, ferry_load, FERRY_CAPACITY);

    // Sonraki FIFO sirasindakini uyar
    if (v->current_side == 'A') pthread_cond_broadcast(&cond_board_A);
    else                         pthread_cond_broadcast(&cond_board_B);

    // Karsiya ulasana kadar bekle
    pthread_cond_wait(&cond_unload, &dock_mutex);

    ferry_load -= v->size;
    v->current_side = (v->current_side == 'A') ? 'B' : 'A';
    printf("[Time %ld] %s-%d unloaded on Side %c\n",
           get_sim_time(), v->type, v->id, v->current_side);

    pthread_mutex_unlock(&dock_mutex);
}

void *vehicle_routine(void *arg) {
    Vehicle *v = (Vehicle *)arg;
    pass_toll_and_board(v);              // Gidis
    usleep((rand() % 1000) * 1000);      // Karsida bekleme
    pass_toll_and_board(v);              // Donus

    pthread_mutex_lock(&stats_mutex);
    completed_vehicles++;
    pthread_mutex_unlock(&stats_mutex);

    printf("[Time %ld] %s-%d completed full round trip.\n",
           get_sim_time(), v->type, v->id);
    return NULL;
}

void *ferry_routine(void *arg) {
    while (completed_vehicles < TOTAL_VEHICLES) {
        // --- Varis ---
        pthread_mutex_lock(&dock_mutex);
        printf("\n[Time %ld] Ferry arrived at Side %c\n",
               get_sim_time(), ferry_side);

        // Once tamamen bosalt
        pthread_cond_broadcast(&cond_unload);
        pthread_mutex_unlock(&dock_mutex);

        usleep(500000);   // Bosaltma penceresi (lock disinda)

        // Sonra boarding ac
        pthread_mutex_lock(&dock_mutex);
        is_boarding = 1;
        total_ferry_trips++;
        if (ferry_side == 'A') pthread_cond_broadcast(&cond_board_A);
        else                    pthread_cond_broadcast(&cond_board_B);
        pthread_mutex_unlock(&dock_mutex);

        usleep(1500000);   // Yukleme penceresi

        // Hareket
        pthread_mutex_lock(&dock_mutex);
        is_boarding = 0;

        pthread_mutex_lock(&stats_mutex);
        total_loaded_units += ferry_load;
        pthread_mutex_unlock(&stats_mutex);

        printf("[Time %ld] Ferry departed from Side %c (Load: %d/%d)\n",
               get_sim_time(), ferry_side, ferry_load, FERRY_CAPACITY);
        ferry_side = (ferry_side == 'A') ? 'B' : 'A';
        pthread_mutex_unlock(&dock_mutex);

        usleep(1000000);   // Gecis suresi
    }
    return NULL;
}

int main(void) {
    sim_start = time(NULL);
    srand((unsigned)time(NULL));

    // Ayarlari oku
    FILE *fp = fopen("ayarlar.txt", "r");
    if (fp) {
        fscanf(fp, "CARS=%d\nMINIBUSES=%d\nTRUCKS=%d\nCAPACITY=%d",
               &NUM_CARS, &NUM_MINIBUSES, &NUM_TRUCKS, &FERRY_CAPACITY);
        fclose(fp);
    } else {
        NUM_CARS = 12; NUM_MINIBUSES = 10; NUM_TRUCKS = 8; FERRY_CAPACITY = 20;
    }
    TOTAL_VEHICLES = NUM_CARS + NUM_MINIBUSES + NUM_TRUCKS;

    // Semafori 2 kapasiteyle baslat (2 gise her tarafta)
    sem_init(&sem_toll_A, 0, 2);
    sem_init(&sem_toll_B, 0, 2);

    // FIX 1: Ferry rastgele taraftan baslasın
    ferry_side = (rand() % 2 == 0) ? 'A' : 'B';

    printf("[Time 0] Simulation started. Ferry begins on Side %c\n", ferry_side);
    printf("[Time 0] Vehicles: %d Cars | %d Minibuses | %d Trucks | Capacity: %d units\n\n",
           NUM_CARS, NUM_MINIBUSES, NUM_TRUCKS, FERRY_CAPACITY);

    pthread_t f_tid;
    pthread_t v_tids[TOTAL_VEHICLES];
    Vehicle   vehicles[TOTAL_VEHICLES];

    pthread_create(&f_tid, NULL, ferry_routine, NULL);

    for (int i = 0; i < TOTAL_VEHICLES; i++) {
        vehicles[i].id = i + 1;
        if      (i < NUM_CARS)                    { strcpy(vehicles[i].type, "Car");     vehicles[i].size = 1; }
        else if (i < NUM_CARS + NUM_MINIBUSES)    { strcpy(vehicles[i].type, "Minibus"); vehicles[i].size = 2; }
        else                                       { strcpy(vehicles[i].type, "Truck");   vehicles[i].size = 3; }

        vehicles[i].start_side   = (rand() % 2 == 0) ? 'A' : 'B';
        vehicles[i].current_side = vehicles[i].start_side;

        // FIX 2 (log): Vehicle creation
        printf("[Time %ld] %s-%d created on Side %c\n",
               get_sim_time(), vehicles[i].type, vehicles[i].id, vehicles[i].start_side);

        pthread_create(&v_tids[i], NULL, vehicle_routine, &vehicles[i]);
    }

    // FIX 2: Once ferry bitsin, sonra tum vehicle thread'leri join et
    pthread_join(f_tid, NULL);
    for (int i = 0; i < TOTAL_VEHICLES; i++)
        pthread_join(v_tids[i], NULL);

    // --- FINAL STATISTICS (REQUIRED) ---
    long sim_time = get_sim_time();
    int  max_possible_units = total_ferry_trips * FERRY_CAPACITY;

    printf("\n============== FINAL STATISTICS ==============\n");
    printf("Total Simulation Time    : %ld seconds\n",   sim_time);
    printf("Total Ferry Trips        : %d\n",             total_ferry_trips);
    printf("Average Wait Time        : %.2f seconds\n",   total_wait_time / (TOTAL_VEHICLES * 2));
    printf("Maximum Wait Time        : %.2f seconds\n",   max_wait_time);
    printf("Ferry Utilization Ratio  : %.2f%%\n",
           max_possible_units > 0
           ? ((double)total_loaded_units / max_possible_units) * 100.0
           : 0.0);
    printf("==============================================\n");

    sem_destroy(&sem_toll_A);
    sem_destroy(&sem_toll_B);
    return 0;
}
