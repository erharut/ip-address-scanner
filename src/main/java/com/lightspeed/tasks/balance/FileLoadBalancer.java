package com.lightspeed.tasks.balance;

import com.lightspeed.tasks.data.ParsingResult;
import com.lightspeed.tasks.libs.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class FileLoadBalancer implements Runnable {
    private final FileChannel fileChannel;
    private final long startPointer;
    private final int size;
    private final int sequenceNumber;
    private final BlockingQueue<String> ipAddressesBlockingQueue;
    private final ConcurrentHashMap<Integer, ParsingResult> parsingResultMap;
    private long rowCount = 0;
    ByteBuffer buff;
    CountDownLatch countDownLatch;

    public FileLoadBalancer(CountDownLatch countDownLatch, FileChannel fileChannel, long startPointer, int size, int sequenceNumber, BlockingQueue<String> ipAddressesBlockingQueue, ConcurrentHashMap<Integer, ParsingResult> parsingResultMap) {
        this.fileChannel = fileChannel;
        this.startPointer = startPointer;
        this.size = size;
        this.sequenceNumber = sequenceNumber;
        this.ipAddressesBlockingQueue = ipAddressesBlockingQueue;
        this.parsingResultMap = parsingResultMap;
        Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            //allocate memory
            buff = ByteBuffer.allocateDirect(size);

            //Read file chunk to RAM
            fileChannel.read(buff, startPointer);
            buff.flip();

            while (buff.hasRemaining()) {
                StringBuilder ip = new StringBuilder();
                char c = (char) buff.get();
                while (c != '\n' && c != '\r') {
                    ip.append(c);
                    if (!buff.hasRemaining()) {
                        break;
                    }
                    c = (char) buff.get();
                }
                if (!ip.isEmpty()) {
                    rowCount++;
                    boolean isValidIp = Utils.isValidIPAddress(ip.toString());
                    if (!isValidIp) {
                        brokenIpStore(ip.toString());
                    } else {
                        if (!brokenIpStore(ip.toString())) {
                            ipAddressesBlockingQueue.put(ip.toString());
                        }
                    }
                }
            }
            buff.clear();

            this.countDownLatch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean brokenIpStore(String ip) {
        if (rowCount == 1) {
            parsingResultMap.putIfAbsent(sequenceNumber, new ParsingResult(ip, ""));
            parsingResultMap.computeIfPresent(sequenceNumber, (k, currentParsingResult) -> new ParsingResult(ip, currentParsingResult.end()));
            return true;
        } else if (!buff.hasRemaining()) {
            parsingResultMap.putIfAbsent(sequenceNumber, new ParsingResult("", ip));
            parsingResultMap.computeIfPresent(sequenceNumber, (k, currentParsingResult) -> new ParsingResult(currentParsingResult.start(), ip));
            return true;
        }
        return false;
    }
}
