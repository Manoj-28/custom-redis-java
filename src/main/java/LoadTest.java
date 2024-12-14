import redis.clients.jedis.Jedis;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;

public class LoadTest {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String redisHost = "localhost";
        int redisPort = 6379;
        int totalRequests = 100000;
        int concurrentClients = 100;

        // Create a thread pool with 'concurrentClients' threads
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentClients);

        // Track completion of tasks
        List<Future<Void>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        // Submit tasks to executor
        for (int i = 0; i < concurrentClients; i++) {
            int clientId = i;
            futures.add(executorService.submit(() -> {
                try (Jedis jedis = new Jedis(redisHost, redisPort)) {
                    for (int j = clientId; j < totalRequests; j += concurrentClients) {
                        jedis.set("key" + j, "value" + j);
                        jedis.get("key" + j);
                    }
                }
                return null; // Return value not needed
            }));
        }

        // Wait for all tasks to complete
        for (Future<Void> future : futures) {
            future.get();
        }

        long endTime = System.currentTimeMillis();
        double throughput = totalRequests / ((endTime - startTime) / 1000.0);

        System.out.println("Throughput: " + throughput + " commands/sec");

        // Shutdown executor service
        executorService.shutdown();
    }
}
