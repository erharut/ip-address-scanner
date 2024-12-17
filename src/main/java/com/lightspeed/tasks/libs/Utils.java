package com.lightspeed.tasks.libs;

import com.lightspeed.tasks.libs.types.MemorySizeTypes;
import com.lightspeed.tasks.libs.types.MemoryTypes;
import com.lightspeed.tasks.libs.types.PercentTypes;
import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Stack;

public class Utils {
    /**
     * @param remain Remain number
     * @param total  Total number
     */
    public static boolean progressBar(long remain, long total) {
        return progressBar(remain, total, 50, '#', "", "");
    }

    /**
     * @param remain Remain number
     * @param total  Total number
     */
    public static boolean progressBar(long remain, long total, String prefix, String suffix) {
        return progressBar(remain, total, 25, '#', prefix, suffix);
    }

    /**
     * @param remain     remain number
     * @param total      total number
     * @param maxBarSize max size to draw a bar
     * @param iconDone   done icon to draw a bar
     */
    public static boolean progressBar(long remain, long total, int maxBarSize, char iconDone, String prefix, String suffix) {
        char defaultIcon = '-';
        if (remain > total | total == 0) {
            throw new IllegalArgumentException();
        }
        float numberPercent = (float) (100 * remain) / total;
        int drawPercent = (int) (numberPercent * maxBarSize / 100.0);

        String bar = new String(new char[maxBarSize]).replace('\0', defaultIcon) + "]";
        String barDone = String.valueOf(iconDone).repeat(drawPercent);
        String barLeft = bar.substring(drawPercent);
        System.out.print("\r" + prefix + "[" + barDone + barLeft + " " + String.format("%.1f", numberPercent) + "%" + suffix);

        if (remain == total) {
            System.out.print("\n");
            return true;
        }
        return false;
    }

    /**
     * @param filePathName Enter the file name
     * @return true if the file exists; false if the file does not exist or its existence cannot be determined.
     */
    public static boolean isValidFile(String filePathName) {
        Path path = FileSystems.getDefault().getPath(filePathName);
        return Files.exists(path);
    }

    /**
     * @param ipAddress IP address
     * @return true if a valid address else false
     */
    public static boolean isValidIPAddress(String ipAddress) {
        if (ipAddress == null) return false;
        String[] ipList = ipAddress.split("\\.");
        if (ipList.length != 4) {
            return false;
        }
        for (String ip : ipList) {
            try {
                if (Integer.parseInt(ip) < 0 || Integer.parseInt(ip) > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param ipAddress IP address
     * @return text IP address to long value ex. 192.168.0.0 to 3232235521
     */
    public static long ipToLong(String ipAddress) {
        if (ipAddress == null) return -1;
        String[] ipAddressInArray = ipAddress.split("\\.");

        long result = 0;
        for (String s : ipAddressInArray) {
            result = result << 8 | Integer.parseInt(s);
        }
        return result;
    }

    /**
     *
     * @param ip the IP type of long convert to text
     * @return the text ip format
     */
    public static String longToIp(long ip) {
        Stack<String> stackIp = new Stack<>();
        for (int i = 0; i <= 3; i++) {
            long value = 255L << 8 * i & ip;
            stackIp.push(String.valueOf(value >> 8 * i));
        }
        StringBuilder stringBuffer = new StringBuilder();
        while (!stackIp.isEmpty()) {
            stringBuffer.append(stackIp.pop()).append(".");
        }
        stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "");
        return stringBuffer.toString();
    }

    /**
     * @param memoryTypes Type of memory
     * @return size of memory, default size is BY
     */
    public static double getMemory(MemoryTypes memoryTypes) {
        return getMemory(memoryTypes, MemorySizeTypes.BY, PercentTypes._100_);
    }

    /**
     * @param memoryTypes     Type of memory
     * @param memorySizeTypes Size type of memory, like: BY,KB,MB,GB,TB
     * @return Size of memory, default size is BY
     */
    public static double getMemory(MemoryTypes memoryTypes, MemorySizeTypes memorySizeTypes) {
        return getMemory(memoryTypes, memorySizeTypes, PercentTypes._100_);
    }

    /**
     * @param memoryTypes     Type of memory
     * @param memorySizeTypes Size type of memory, like: BY,KB,MB,GB,TB
     * @param percentAmount   Get a percentage of the memory amount
     * @return Size of memory
     */
    public static double getMemory(MemoryTypes memoryTypes, MemorySizeTypes memorySizeTypes, PercentTypes percentAmount) {
        long byteData = 1L << memorySizeTypes.getValue();
        long result = switch (memoryTypes) {
            case FREE_SYSTEM_MEMORY ->
                    ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getFreeMemorySize();
            case TOTAL_SYSTEM_MEMORY ->
                    ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalMemorySize();
            case FREE_VM_MEMORY -> Runtime.getRuntime().freeMemory();
            case TOTAL_VM_MEMORY -> Runtime.getRuntime().totalMemory();
        };
        result = (long) (result * percentAmount.getValue() / 100.0);
        return result / (double) byteData;
    }

    /**
     * @param filePathName Input a file name path.
     * @param readCount When the number of bytes read from a file is less than the designated read count, use the actual chunk size as the value instead.
     * @return Optimal thread pool count.
     */
    public static int calculateOptimalIOThreadCount(String filePathName, int readCount) {
        return calculateOptimalIOThreadCount(filePathName, readCount, 0.8f, 2.0f);
    }

    /**
     * @param filePathName Input a file name path.
     * @param readCount    When the number of bytes read from a file is less than the designated read count, use the actual chunk size as the value instead.
     * @param cpuLoad      CPU loader percent definition a between 0(0%) <= Used CPU(%) >= 1(100%),
     * @param businessLoad Calculate an average time of business logic.
     * @return Optimal thread pool count.
     */
    public static int calculateOptimalIOThreadCount(String filePathName, int readCount, float cpuLoad, float businessLoad) {
        if (cpuLoad < 0.0 && cpuLoad > 1.0) {
            throw new IllegalArgumentException();
        }
        System.out.println("------------------------- < Calculation Info > -------------------------");
        System.out.println("Preparing to find optimal I/O Thread Pool Count ... ");
        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePathName), StandardOpenOption.READ)) {
            long remainingSize = fileChannel.size();
            long chunkSize = InitConfig.DEFAULT.fileReadChunkSize();
            long totalCountOfChunks = fileChannel.size() / InitConfig.DEFAULT.fileReadChunkSize();
            long startPointer = 0;//file pointer
            long averageTimeUsedIOCPU = 0;
            int chunkCount = 0;

            totalCountOfChunks = totalCountOfChunks < readCount ? totalCountOfChunks : readCount;
            do {
                long startTime = System.currentTimeMillis();

                ByteBuffer buff = ByteBuffer.allocateDirect(Math.toIntExact(chunkSize));

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
                        Utils.isValidIPAddress(ip.toString());
                    }
                }
                buff.clear();
                startPointer = (long) (Math.random() * remainingSize / 2);
                averageTimeUsedIOCPU += System.currentTimeMillis() - startTime;
            } while (totalCountOfChunks >= chunkCount++);

            long numberOfCPUCoreSize = Runtime.getRuntime().availableProcessors();
            averageTimeUsedIOCPU = (long) (averageTimeUsedIOCPU / (float) chunkCount);
            return (int) (numberOfCPUCoreSize * cpuLoad * (1 + averageTimeUsedIOCPU / businessLoad));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param timerSeconds Spend a seconds
     * @return The string time default format is dd:hh:mm:ss
     */
    public static String convertToTime(long timerSeconds) {
        return convertToTime(timerSeconds, false);
    }

    /**
     * @param timerSeconds  Spend a seconds
     * @param isShortFormat If true the time format is dd:hh:mm:ss else Days:dd Hours:hh Minutes:mm Seconds:ss
     * @return The string time format dd:hh:mm:ss
     */
    public static String convertToTime(long timerSeconds, boolean isShortFormat) {
        Long[] dataList = new Long[4];
        long currentValue = timerSeconds;
        int dividerValue = 3600 * 24;
        int i = 0;
        while (dividerValue > 0) {
            dataList[i++] = currentValue / dividerValue;
            currentValue = currentValue % dividerValue;
            dividerValue = (int) Math.pow(60, dataList.length - (i + 1));
        }
        if (isShortFormat) {
            return String.format("%02d:%02d:%02d:%02d", Arrays.stream(dataList).toArray());
        } else {
            return String.format("Days:%02d Hours:%02d Minutes:%02d Seconds:%02d", Arrays.stream(dataList).toArray());
        }
    }
}
