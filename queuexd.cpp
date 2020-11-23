#include <stdio.h>
#include <stdlib.h>
#include "queue.h"

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

