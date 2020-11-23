#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include "queue.h"

struct queue_node_s {
   int src;
   int mesg;
   struct queue_node_s* next_p;
};

struct queue_s{
   int enqueued;
   int dequeued;
   struct queue_node_s* front_p;
   struct queue_node_s* tail_p;
};

struct queue_s* Allocate_queue(void);
void Free_queue(struct queue_s* q_p);
void Print_queue(struct queue_s* q_p);
void Enqueue(struct queue_s* q_p, int src, int mesg);
int Dequeue(struct queue_s* q_p, int* src_p, int* mesg_p);
int Search(struct queue_s* q_p, int mesg, int* src_p);
const int MAX_MSG = 10000;
struct queue_s* Allocate_queue() {
   struct queue_s* q_p = new(struct queue_s);
   q_p->enqueued = q_p->dequeued = 0;
   q_p->front_p = NULL;
   q_p->tail_p = NULL;
   return q_p;
}  

void Free_queue(struct queue_s* q_p) {
   struct queue_node_s* curr_p = q_p->front_p;
   struct queue_node_s* temp_p;

   while(curr_p != NULL) {
      temp_p = curr_p;
      curr_p = curr_p->next_p;
      free(temp_p);
   }
   q_p->enqueued = q_p->dequeued = 0;
   q_p->front_p = q_p->tail_p = NULL;
}   

void Print_queue(struct queue_s* q_p) {
   struct queue_node_s* curr_p = q_p->front_p;

   printf("queue = \n");
   while(curr_p != NULL) {
      printf("   src = %d, mesg = %d\n", curr_p->src, curr_p->mesg);
      curr_p = curr_p->next_p;
   }
   printf("puesto en cola = %d, retirado de la cola = %d\n", q_p->enqueued, q_p->dequeued);
   printf("\n");
}  

void Enqueue(struct queue_s* q_p, int src, int mesg) {
   struct queue_node_s* n_p = new(struct queue_node_s);
   n_p->src = src;
   n_p->mesg = mesg;
   n_p->next_p = NULL;
   if (q_p->tail_p == NULL) { 
      q_p->front_p = n_p;
      q_p->tail_p = n_p;
   } else {
      q_p->tail_p->next_p = n_p;
      q_p->tail_p = n_p;
   }
   q_p->enqueued++;
}  

int Dequeue(struct queue_s* q_p, int* src_p, int* mesg_p) {
   struct queue_node_s* temp_p;

   if (q_p->front_p == NULL) return 0;
   *src_p = q_p->front_p->src;
   *mesg_p = q_p->front_p->mesg;
   temp_p = q_p->front_p;
   if (q_p->front_p == q_p->tail_p) 
      q_p->front_p = q_p->tail_p = NULL;
   else
      q_p->front_p = temp_p->next_p;
   free(temp_p);
   q_p->dequeued++;
   return 1;
}  

int Search(struct queue_s* q_p, int mesg, int* src_p) {
   struct queue_node_s* curr_p = q_p->front_p;

   while (curr_p != NULL)
      if (curr_p->mesg == mesg) {
         *src_p = curr_p->src;
         return 1;
      } else {
         curr_p = curr_p->next_p;
      }
   return 0;

}  


#ifdef USE_MAIN
int main(void) {
   char op;
   int  src, mesg;
   struct queue_s* q_p = Allocate_queue();

   printf("Operacion? (e -> poner en cola, d -> retirar de la cola, p -> imprimir, s -> buscar, f -> liberar, q -> salir)\n");
   scanf(" %c", &op);
   while (op != 'q' && op != 'Q') {
      switch (op) {
         case 'e':
         case 'E':
            printf("Src? Mesg?\n");
            scanf("%d%d", &src, &mesg);
            Enqueue(q_p, src, mesg);
            break;
         case 'd':
         case 'D':
            if (Dequeue(q_p, &src, &mesg))
               printf("Retirado de la cola src = %d, mesg = %d\n", src, mesg);
            else 
               printf("Queue vacia\n");
            break;
         case 's':
         case 'S':
            printf("Mesg?\n");
            scanf("%d", &mesg);
            if (Search(q_p, mesg, &src))
               printf("Encontrado %d desde %d\n", mesg, src);
            else
               printf("No encontrado %d\n", mesg);
            break;
         case 'p':
         case 'P':
            Print_queue(q_p);
            break;
         case 'f':
         case 'F':
            Free_queue(q_p);
            break;
         default:
            printf("%c Comando no valido\n", op);
            printf("Intente de nuevo\n");
      }  
      printf("Operacion? (e -> poner en cola, d -> retirar de la cola, p -> imprimir, s -> buscar, f -> liberar, q -> salir)\n");
      scanf(" %c", &op);
   }  

   Free_queue(q_p);
   free(q_p);
   return 0;
}  
#endif

void Usage(char *prog_name) {
   fprintf(stderr, "uso: %s <numero de hilos> <numero de mensajes>\n",
         prog_name);
   fprintf(stderr, "numero de mensajes = numero enviado por cada hilo\n");
   exit(0);
}  


void Send_msg(struct queue_s* msg_queues[], int my_rank, 
   int thread_count, int msg_number) {
   int mesg = -msg_number;
   int dest = random() % thread_count;

#  pragma omp critical
   
   Enqueue(msg_queues[dest], my_rank, mesg);
#  ifdef DEBUG
   printf("Hilo %d > enviado %d a %d\n", my_rank, mesg, dest);
#  endif
}  

void Try_receive(struct queue_s* q_p, int my_rank) {
   int src, mesg;
   int queue_size = q_p->enqueued - q_p->dequeued;

   if (queue_size == 0) return;
   else if (queue_size == 1)
#     pragma omp critical
      Dequeue(q_p, &src, &mesg);  
   else
      Dequeue(q_p, &src, &mesg);
   printf("Hilo %d > recibido %d desde %d\n", my_rank, mesg, src);
}   


int Done(struct queue_s* q_p, int done_sending, int thread_count) {
   int queue_size = q_p->enqueued - q_p->dequeued;
   if (queue_size == 0 && done_sending == thread_count)
      return 1;
   else 
      return 0;
}   


int main(int argc, char* argv[]) {
   int thread_count;
   int send_max;
   struct queue_s** msg_queues;
   int done_sending = 0;

   if (argc != 3) Usage(argv[0]);
   thread_count = strtol(argv[1], NULL, 10);
   send_max = strtol(argv[2], NULL, 10);
   if (thread_count <= 0 || send_max < 0) Usage(argv[0]);

   msg_queues = malloc(thread_count*sizeof(struct queue_node_s*));

#  pragma omp parallel num_threads(thread_count) \
      default(none) shared(thread_count, send_max, msg_queues, done_sending)
   {
      int my_rank = omp_get_thread_num();
      int msg_number;
      srandom(my_rank);
      msg_queues[my_rank] = Allocate_queue();

#     pragma omp barrier 
                         

      for (msg_number = 0; msg_number < send_max; msg_number++) {
         Send_msg(msg_queues, my_rank, thread_count, msg_number);
         Try_receive(msg_queues[my_rank], my_rank);
      }

#     pragma omp atomic

      done_sending++;
#     ifdef DEBUG
      printf("Hilo %d > envio realizado\n", my_rank);
#     endif

      while (!Done(msg_queues[my_rank], done_sending, thread_count))
         Try_receive(msg_queues[my_rank], my_rank);


      Free_queue(msg_queues[my_rank]);
      free(msg_queues[my_rank]);
   }  

   free(msg_queues);
   return 0;
}  


