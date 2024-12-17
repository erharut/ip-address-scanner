package com.lightspeed.tasks.balance;

import com.lightspeed.tasks.libs.Utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class CPUBalancer implements Runnable {
    private static final long PART_VALUE = Math.ceilDiv(0xffffffffL, 63);
    private static final AtomicLongArray dataBitArray = new AtomicLongArray((int) PART_VALUE);
    private final BlockingQueue<String> ipAddressesBlockingQueue;
    private final AtomicLong countOfUniqueIPAddresses;
    private final AtomicLong totalCountOfIPAddresses;

    public CPUBalancer(BlockingQueue<String> ipAddressesBlockingQueue, AtomicLong countOfUniqueIPAddresses, AtomicLong totalCountOfIPAddresses) {
        this.ipAddressesBlockingQueue = ipAddressesBlockingQueue;
        this.countOfUniqueIPAddresses = countOfUniqueIPAddresses;
        this.totalCountOfIPAddresses = totalCountOfIPAddresses;
    }

    @Override
    public void run() {
        String line;
        while (true) {
            try {
                line = ipAddressesBlockingQueue.take();
                this.setBitValue(Utils.ipToLong(line));
            } catch (InterruptedException e) {
                break; // FileTask has completed
            }
        }
    }

    private void setBitValue(long ipValue) {
        if (ipValue > 0) {
            int segmentPosition = (int) Math.ceilDiv(ipValue, 63L) - 1;
            long newData = (1L << (ipValue - segmentPosition * 63L) - 1);
            if ((newData & dataBitArray.get(segmentPosition)) == 0) {
                dataBitArray.getAndUpdate(segmentPosition, operand -> operand | newData);
                this.countOfUniqueIPAddresses.getAndIncrement();
            }
            totalCountOfIPAddresses.getAndIncrement();
        }
    }
}
