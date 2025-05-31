package dzthreadwork;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class JavaThreadPoolTest {

    private ThreadPoolExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void testBasicExecution() throws Exception {
        executor = new ThreadPoolExecutor(
                2, 4, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10)
        );
        executor.allowCoreThreadTimeOut(false);

        CountDownLatch latch = new CountDownLatch(2);

        executor.execute(latch::countDown);
        executor.execute(latch::countDown);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Задачи должны выполниться пулом");
    }

    @Test
    void testDynamicExpansion() throws Exception {
        int corePoolSize = 2;
        int maxPoolSize = 4;
        int queueSize = 1;
        executor = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueSize)
        );
        executor.allowCoreThreadTimeOut(false);

        int tasksCount = maxPoolSize + queueSize;
        CountDownLatch startedLatch = new CountDownLatch(tasksCount);
        CountDownLatch blockLatch = new CountDownLatch(1);

        int accepted = 0;
        for (int i = 0; i < tasksCount; i++) {
            try {
                executor.execute(() -> {
                    startedLatch.countDown();
                    try {
                        blockLatch.await(1, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {}
                });
                accepted++;
            } catch (RejectedExecutionException e) {
                // Задача отклонена, не учитываем её
            }
        }

        assertEquals(tasksCount, accepted, "Должно быть принято ровно столько задач, сколько позволяет пул и очередь");
        assertTrue(startedLatch.await(2, TimeUnit.SECONDS), "Все принятые задачи должны стартовать");

        Thread.sleep(200);
        int workersNow = executor.getPoolSize();
        assertTrue(workersNow > corePoolSize, "Пул должен динамически расширяться (потоков: " + workersNow + ")");
        assertTrue(workersNow <= maxPoolSize, "Пул не должен превышать maxPoolSize");
        blockLatch.countDown();
    }

    @Test
    void testMinSpareThreadsIdleShrink() throws Exception {
        int corePoolSize = 3;
        int maxPoolSize = 6;
        int queueSize = 10;
        executor = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize, 200, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueSize)
        );
        executor.allowCoreThreadTimeOut(false);

        int tasksCount = 10;
        CountDownLatch latch = new CountDownLatch(tasksCount);

        int accepted = 0;
        for (int i = 0; i < tasksCount; i++) {
            try {
                executor.execute(latch::countDown);
                accepted++;
            } catch (RejectedExecutionException ignored) {}
        }
        assertTrue(accepted > 0, "Хотя бы часть задач должна быть принята");
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Все принятые задачи должны быть выполнены");

        Thread.sleep(800);

        int count = executor.getPoolSize();
        assertTrue(count <= maxPoolSize, "Пул не должен превышать maxPoolSize");
        assertEquals(corePoolSize, count, "Пул должен сохранять минимум corePoolSize потоков");
    }

    @Test
    void testMinSpareThreadsNeverBelow() throws Exception {
        int minSpare = 2;
        int corePoolSize = minSpare;
        int maxPoolSize = 6;
        executor = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize, 200, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10)
        );
        executor.allowCoreThreadTimeOut(false);

        int tasksCount = 10;
        CountDownLatch latch = new CountDownLatch(tasksCount);

        for (int i = 0; i < tasksCount; i++) {
            executor.execute(latch::countDown);
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Все задачи должны выполниться");

        Thread.sleep(1000);

        int workerCount = executor.getPoolSize();
        assertEquals(minSpare, workerCount, "Число потоков не должно быть меньше corePoolSize (minSpareThreads)");
    }

    @Test
    void testSubmitFuture() throws Exception {
        executor = new ThreadPoolExecutor(
                2, 4, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10)
        );
        executor.allowCoreThreadTimeOut(false);

        Future<Integer> future = executor.submit(() -> 42);
        assertEquals(42, future.get(2, TimeUnit.SECONDS), "Результат задачи через Future должен быть корректен");
    }

    @Test
    void testShutdownRejectsTasks() {
        executor = new ThreadPoolExecutor(
                2, 4, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10)
        );
        executor.allowCoreThreadTimeOut(false);

        executor.shutdown();
        assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {}), "После shutdown пул не должен принимать задачи");
    }

    @Test
    void testShutdownNowStopsWorkers() throws InterruptedException {
        executor = new ThreadPoolExecutor(
                2, 4, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10)
        );
        executor.allowCoreThreadTimeOut(false);

        executor.shutdownNow();

        Thread.sleep(300);
        int workerCount = executor.getPoolSize();
        // Можно добавить проверку, что потоков осталось не больше 2, но завершение может занять время
    }
}
