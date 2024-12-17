package com.lightspeed.tasks.balance;

import com.lightspeed.tasks.libs.Utils;
import com.lightspeed.tasks.libs.types.MemorySizeTypes;
import com.lightspeed.tasks.libs.types.MemoryTypes;
import com.lightspeed.tasks.libs.types.PercentTypes;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class InterruptionThreadPoolExecutor extends ThreadPoolExecutor {
    private boolean isInterrupted;
    private final ReentrantLock lock;
    private final Condition condition;

    /**
     *
     */
    private static class MemoryMonitoringTool implements Runnable {
        private final PercentTypes lowerLimitOfFreeMemPercent;
        private double lockMinFreeMemorySize;
        private final InterruptionThreadPoolExecutor interruptionThreadPoolExecutor;
        private final CountDownLatch countDownLatch;
        private final long totalCount;
        private final AtomicBoolean isComplete = new AtomicBoolean(false);
        private long lastDoneCount = 0;

        public MemoryMonitoringTool(InterruptionThreadPoolExecutor interruptionThreadPoolExecutor, PercentTypes lowerLimitOfFreeMemPercent, CountDownLatch countDownLatch) {
            this.interruptionThreadPoolExecutor = interruptionThreadPoolExecutor;
            this.lowerLimitOfFreeMemPercent = lowerLimitOfFreeMemPercent;
            this.countDownLatch = countDownLatch;
            this.totalCount = countDownLatch.getCount();
            this.lockMinFreeMemorySize = Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.BY, this.lowerLimitOfFreeMemPercent);
            double maxFreeMemory = Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.GB);
            System.out.printf("Lock a free Memory size (GB): [%.2f] of [%.2f]\n", maxFreeMemory - lockMinFreeMemorySize / 1024 / 1024 / 1024, maxFreeMemory);
        }

        @Override
        public void run() {
            String prefix = "\u001B[32m<Loading>\u001B[0m";
            double spendTime = 0;
            double remainingTime = 0;
            long lastTimer = System.currentTimeMillis();

            while (!isComplete.get()) {

                double currentFreeMem = Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.GB);
                long currentDoneCount = this.totalCount - countDownLatch.getCount();
                if (spendTime > 0) {
                    remainingTime = countDownLatch.getCount() / (float) currentDoneCount * spendTime;
                }
                String genericInfo = String.format(" (%s/%s):\u001B[34m(%c)\u001B[0m (DONE: \u001B[32m%d\u001B[0m | TODO: \u001B[33m%d\u001B[0m)",
                        Utils.convertToTime((long) spendTime, true), Utils.convertToTime((long) remainingTime, true),
                        "|/-\\".charAt((int) (Math.random() * 4)),
                        currentDoneCount, countDownLatch.getCount()) +
                        String.format(" #Free Memory (GB):[%.2f] of [%.2f]", currentFreeMem - lockMinFreeMemorySize / 1024 / 1024 / 1024, currentFreeMem);

                if (interruptionThreadPoolExecutor.isRunning() && checkIsFullMemory()) {
                    interruptionThreadPoolExecutor.interrupt();
                    prefix = "\u001B[31m<Clearing>\u001B[0m";
                } else if (interruptionThreadPoolExecutor.isInterrupted() && checkIsEmptyMemory()) {
                    //Preparing to check the actual free memory and update limits
                    lockMinFreeMemorySize = Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.BY, this.lowerLimitOfFreeMemPercent);
                    interruptionThreadPoolExecutor.resume();
                    prefix = "\u001B[33m<Loading>\u001B[0m";

                }

                if (lastDoneCount != currentDoneCount) {
                    lastDoneCount = currentDoneCount;
                    spendTime += (System.currentTimeMillis() - lastTimer) / 1000.0;
                    lastTimer = System.currentTimeMillis();
                }
                //TODO for short version
                //System.out.print("\r" + prefix + genericInfo);
                //isComplete.set(currentDoneCount == this.totalCount);
                //TODO for full version
                isComplete.set(Utils.progressBar(currentDoneCount, this.totalCount
                        , prefix
                        , genericInfo));
                /*
                    If it doesn't need a delay you can remove it when increasing a delay could cause
                    an incorrect view of the percentage.
                 */
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }


            System.out.println(interruptionThreadPoolExecutor.getCompletedTaskCount() + " " + interruptionThreadPoolExecutor.getTaskCount());
            System.out.println("Done Memory Manager");
        }

        /**
         *
         * @return true if the memory is empty
         */
        private boolean checkIsEmptyMemory() {
            if (interruptionThreadPoolExecutor.getActiveCount() - interruptionThreadPoolExecutor.countOfWaiting() == 0 && countDownLatch.getCount() > 0) {
                System.out.println("\nMemory empty: Preparing to reload ...");
            }
            return interruptionThreadPoolExecutor.getActiveCount() - interruptionThreadPoolExecutor.countOfWaiting() == 0 && countDownLatch.getCount() > 0;

        }

        /**
         *
         * @return true if memory is full
         */
        private boolean checkIsFullMemory() {
            double currentFreeSize = Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.BY);
            if (currentFreeSize < lockMinFreeMemorySize) {
                System.out.println("\nMemory full: Preparing to clean memory ...");
            }
            return currentFreeSize < lockMinFreeMemorySize;
        }
    }


    private InterruptionThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    public static InterruptionThreadPoolExecutor newFixedThreadPool(int nThreads, ThreadFactory threadFactory, PercentTypes lowerLimitOfFreeMemPercent, CountDownLatch countDownLatch) {
        InterruptionThreadPoolExecutor interruptionThreadPoolExecutor = new InterruptionThreadPoolExecutor(nThreads, nThreads,
                10L, TimeUnit.HOURS,
                new LinkedBlockingQueue<>(),
                threadFactory);
        Thread.startVirtualThread(new MemoryMonitoringTool(interruptionThreadPoolExecutor, lowerLimitOfFreeMemPercent, countDownLatch))
                .setPriority(Thread.MIN_PRIORITY);
        return interruptionThreadPoolExecutor;
    }

    public int countOfWaiting() {
        lock.lock();
        try {
            return lock.getWaitQueueLength(condition);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param thread   The thread being executed
     * @param runnable The runnable task
     * @see {@link ThreadPoolExecutor#beforeExecute(Thread, Runnable)}
     */
    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        lock.lock();
        try {
            while (isInterrupted) {
                condition.await();
            }
        } catch (InterruptedException ie) {
            thread.interrupt();
        } finally {
            lock.unlock();
        }
    }

    public boolean isRunning() {
        return !isInterrupted;
    }

    public boolean isInterrupted() {
        return isInterrupted;
    }

    /**
     * Pause the execution
     */
    public void interrupt() {
        lock.lock();
        try {
            isInterrupted = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resume pool execution
     */
    public void resume() {
        lock.lock();
        try {
            isInterrupted = false;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

}
