#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <omp.h> 

void Usage(char* prog_name) {
   fprintf(stderr, "usage: %s <thread_count> <n>\n", prog_name);  /* Change */
   fprintf(stderr, "   thread_count is the number of threads >= 1\n");  /* Change */
   fprintf(stderr, "   n is the number of terms and should be >= 1\n");
   exit(0);
}  

int main(int argc, char* argv[]) {
   long long n, i;
   int thread_count;
   double factor;
   double sum = 0.0;

   if (argc != 3) Usage(argv[0]);
   thread_count = strtol(argv[1], NULL, 10);
   n = strtoll(argv[2], NULL, 10);
   if (thread_count < 1 || n < 1) Usage(argv[0]);

#  pragma omp parallel for num_threads(thread_count) \
      reduction(+: sum) private(factor)
   for (i = 0; i < n; i++) {
      factor = (i % 2 == 0) ? 1.0 : -1.0; 
      sum += factor/(2*i+1);
#     ifdef DEBUG
      printf("Hilo %d > i = %lld, sum = %f\n", my_rank, i, my_sum);
#     endif
   }

   sum = 4.0*sum;
   printf("With n = %lld terminos and %d hilos,\n", n, thread_count);
   printf("Pi estimado = %.14f\n", sum);
   return 0;
}  /* main */

