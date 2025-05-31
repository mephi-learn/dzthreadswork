package dzthreadwork;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueExecutor implements CustomExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MultiQueueExecutor.class);

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int minSpareThreads;

    private final BlockingQueue<Runnable> taskQueue;
    private final List<Worker> workers = new ArrayList<>();
    private final Object workersLock = new Object();
    private final AtomicInteger threadCounter = new AtomicInteger(0);

    private volatile boolean isShutdown = false;

    public MultiQueueExecutor(int corePoolSize, int maxPoolSize, int queueSize,
                              long keepAliveTime, TimeUnit timeUnit, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueSize = queueSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.minSpareThreads = minSpareThreads;

        this.taskQueue = new LinkedBlockingQueue<>(this.queueSize);

        logger.info("Инициализация пула потоков: core={}, max={}, queueSize={}, keepAlive={} {}, minSpare={}",
                corePoolSize, maxPoolSize, queueSize, keepAliveTime, timeUnit.name(), minSpareThreads);

        for (int i = 0; i < corePoolSize; i++) {
            addWorker(null);
        }
    }

    private void addWorker(Runnable firstTask) {
        Worker worker = new Worker(firstTask);
        synchronized (workersLock) {
            workers.add(worker);
        }
        Thread t = new Thread(worker, "Worker-" + threadCounter.getAndIncrement());
        t.start();
        logger.info("Поток {} создан. Всего потоков: {}", t.getName(), getWorkerCount());
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            throw new RejectedExecutionException("Пул потоков завершён, задача отклонена.");
        }

        boolean added = false;
        synchronized (workersLock) {
            if (workers.size() < corePoolSize) {
                addWorker(command);
                added = true;
            } else {
                added = taskQueue.offer(command);
                if (!added && workers.size() < maxPoolSize) {
                    addWorker(command);
                    added = true;
                }
            }
        }
        if (!added) {
            throw new RejectedExecutionException("Очередь переполнена, задача отклонена.");
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (isShutdown) {
            throw new RejectedExecutionException("Пул потоков завершён, задача отклонена.");
        }
        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        if (isShutdown) return;
        isShutdown = true;
        logger.info("Пул переводится в режим завершения (shutdown)");
    }

    @Override
    public void shutdownNow() {
        if (isShutdown) return;
        isShutdown = true;
        logger.warn("Принудительное завершение всех потоков (shutdownNow). Текущий размер пула: {}", getWorkerCount());
        synchronized (workersLock) {
            for (Worker worker : workers) {
                worker.stop();
            }
        }
        taskQueue.clear();
    }

    public int getWorkerCount() {
        synchronized (workersLock) {
            return workers.size();
        }
    }

    private void removeWorker(Worker worker) {
        synchronized (workersLock) {
            workers.remove(worker);
        }
    }

    private class Worker implements Runnable {
        private volatile boolean running = true;
        private final Runnable firstTask;

        public Worker(Runnable firstTask) {
            this.firstTask = firstTask;
        }

        public void stop() {
            running = false;
        }

        @Override
        public void run() {
            try {
                Runnable task = firstTask;
                while (running && !Thread.currentThread().isInterrupted()) {
                    if (task != null) {
                        try {
                            task.run();
                        } catch (Exception e) {
                            logger.error("Ошибка выполнения задачи: ", e);
                        }
                        task = null;
                    } else {
                        task = taskQueue.poll(keepAliveTime, timeUnit);
                        if (task == null) {
                            boolean shouldExit = false;
                            synchronized (workersLock) {
                                if (workers.size() > minSpareThreads) {
                                    shouldExit = true;
                                    removeWorker(this);
                                    logger.info("Поток {} завершён из-за простоя. Осталось потоков: {}",
                                            Thread.currentThread().getName(), workers.size());
                                }
                            }
                            if (shouldExit) break;
                        }
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
