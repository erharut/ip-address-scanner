package com.lightspeed.tasks.balance;

import com.lightspeed.tasks.data.AddressesResults;
import com.lightspeed.tasks.data.ParsingResult;
import com.lightspeed.tasks.libs.InitConfig;
import com.lightspeed.tasks.libs.Utils;
import com.lightspeed.tasks.libs.types.PercentTypes;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class BalancerManager {
    private static final BlockingQueue<String> ipAddressesBlockingQueue = new ArrayBlockingQueue<>(InitConfig.DEFAULT.steamShareBlockSize());
    private static final AtomicLong countOfUniqueIPAddresses = new AtomicLong(0);
    private static final AtomicLong totalCountOfIPAddresses = new AtomicLong(0);
    private final PercentTypes lowerLimitOfFreeMemPercent;
    private final String fileName;
    private final ConcurrentHashMap<Integer, ParsingResult> parsingResultMap = new ConcurrentHashMap<>();
    private final int optimalThreadPoolSize;

    /**
     *
     */
    public static class BalancerManagerBuilder {
        private final String fileName;
        private PercentTypes grabOfFreeMemoryPercent = PercentTypes._80_;
        private PercentTypes lowerLimitOfFreeMemPercent = PercentTypes.fromValue(100 - grabOfFreeMemoryPercent.getValue());
        private int optimalThreadPoolSize;
        private int optimalThreadPoolReadCount = 100;

        public BalancerManagerBuilder(String fileName) {
            this.fileName = fileName;
        }

        /**
         *
         * @param grabOfFreeMemoryPercent The percentage of memory to allocate for all processes.
         * @return {@link BalancerManagerBuilder}
         */
        public BalancerManagerBuilder setGrabOfFreeMemoryPercent(PercentTypes grabOfFreeMemoryPercent) {
            this.grabOfFreeMemoryPercent = grabOfFreeMemoryPercent;
            return this;
        }

        /**
         *
         * @param optimalThreadPoolReadCount Count the number of times a file is read randomly to determine the optimal thread pool.
         * @return {@link BalancerManagerBuilder}
         */
        public BalancerManagerBuilder setOptimalThreadPoolReadCount(int optimalThreadPoolReadCount) {
            this.optimalThreadPoolReadCount = optimalThreadPoolReadCount;
            return this;
        }

        public BalancerManager build() {
            this.lowerLimitOfFreeMemPercent = PercentTypes.fromValue(100 - grabOfFreeMemoryPercent.getValue());
            optimalThreadPoolSize = Utils.calculateOptimalIOThreadCount(fileName, optimalThreadPoolReadCount);
            return new BalancerManager(this);
        }
    }

    private BalancerManager(BalancerManagerBuilder builder) {
        this.fileName = builder.fileName;
        this.lowerLimitOfFreeMemPercent = builder.lowerLimitOfFreeMemPercent;
        this.optimalThreadPoolSize = builder.optimalThreadPoolSize;
    }

    public AddressesResults runScanner() {
        try (FileChannel channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ)) {
            long remainingSize = channel.size(); //get the total number of bytes in the file
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < optimalThreadPoolSize + 1; i++) {
                    executor.submit(new CPUBalancer(ipAddressesBlockingQueue, countOfUniqueIPAddresses, totalCountOfIPAddresses));
                }
                long chunkSize = InitConfig.DEFAULT.fileReadChunkSize();
                long chunkCount = Math.ceilDiv(remainingSize, chunkSize);
                CountDownLatch countDownLatch = new CountDownLatch((int) chunkCount);
                System.out.println("Optimal Thread Pool Count is: " + optimalThreadPoolSize);
                try (InterruptionThreadPoolExecutor interruptionThreadPoolExecutor = InterruptionThreadPoolExecutor.newFixedThreadPool(optimalThreadPoolSize, Thread.ofVirtual().factory(), lowerLimitOfFreeMemPercent, countDownLatch)) {
                    long startPoint = 0;//file pointer
                    int i = 0; //loop counter
                    while (remainingSize >= chunkSize) {
                        interruptionThreadPoolExecutor.submit(new FileLoadBalancer(countDownLatch, channel, startPoint, Math.toIntExact(chunkSize), i++, ipAddressesBlockingQueue, parsingResultMap));
                        remainingSize = remainingSize - chunkSize;
                        startPoint = startPoint + chunkSize;
                    }
                    //loading the last remaining piece
                    interruptionThreadPoolExecutor.submit(new FileLoadBalancer(countDownLatch, channel, startPoint, Math.toIntExact(remainingSize), i, ipAddressesBlockingQueue, parsingResultMap));
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                while (!ipAddressesBlockingQueue.isEmpty()) {
                    System.out.println("Waiting... " + ipAddressesBlockingQueue.size());
                }

                //Preparing to join a split IPs
                try (ExecutorService service = Executors.newVirtualThreadPerTaskExecutor()) {
                    service.submit(() -> {
                        for (int sequenceNumber = 0; sequenceNumber < parsingResultMap.size() - 1; sequenceNumber++) {
                            ParsingResult currentParsingResult = parsingResultMap.get(sequenceNumber);

                            try {

                                if (sequenceNumber == 0) {
                                    ipAddressesBlockingQueue.put(currentParsingResult.start());
                                }

                                ParsingResult nextParsingResult = parsingResultMap.get(sequenceNumber + 1);
                                if (Utils.isValidIPAddress(currentParsingResult.end()) && Utils.isValidIPAddress(nextParsingResult.start())) {
                                    ipAddressesBlockingQueue.put(nextParsingResult.start());
                                    ipAddressesBlockingQueue.put(currentParsingResult.end());
                                } else {
                                    String margeIP = currentParsingResult.end() + nextParsingResult.start();
                                    ipAddressesBlockingQueue.put(margeIP);
                                }
                                if (sequenceNumber + 1 == parsingResultMap.size() - 1) {
                                    ipAddressesBlockingQueue.put(nextParsingResult.end());
                                }
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }).get();
                } catch (ExecutionException | InterruptedException e) {
                    System.out.println(e.getMessage());
                    throw new RuntimeException(e);
                }


                while (!ipAddressesBlockingQueue.isEmpty()) {
                    System.out.println("Waiting... " + ipAddressesBlockingQueue.size());
                }

                executor.shutdownNow();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new AddressesResults(countOfUniqueIPAddresses.get(), totalCountOfIPAddresses.get());
    }


}
