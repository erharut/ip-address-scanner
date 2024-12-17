package com.lightspeed.tasks;

import com.lightspeed.tasks.balance.BalancerManager;
import com.lightspeed.tasks.data.AddressesResults;
import com.lightspeed.tasks.libs.Utils;
import com.lightspeed.tasks.libs.types.MemorySizeTypes;
import com.lightspeed.tasks.libs.types.MemoryTypes;
import com.lightspeed.tasks.libs.types.PercentTypes;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.print("Please enter the file to read: ");
        String pathFile = in.nextLine();
        System.out.println("Preparing to scan the current file: " + pathFile);
        if (!Utils.isValidFile(pathFile) | pathFile.isEmpty()) {
            System.out.println("Invalid file: " + pathFile);
        } else {

            System.out.println("------------------------- < Buffer Info > -------------------------");
            System.out.printf("BEFORE ---- VM free memory: %.2f GB of %.2f GB \n",
                    Utils.getMemory(MemoryTypes.FREE_VM_MEMORY, MemorySizeTypes.GB),
                    Utils.getMemory(MemoryTypes.TOTAL_VM_MEMORY, MemorySizeTypes.GB));
            System.out.printf("BEFORE ---- SYSTEM free memory: %.2f GB of %.2f GB \n",
                    Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.GB),
                    Utils.getMemory(MemoryTypes.TOTAL_SYSTEM_MEMORY, MemorySizeTypes.GB));

            BalancerManager balancerManager = new BalancerManager.BalancerManagerBuilder(pathFile)
                    .setGrabOfFreeMemoryPercent(PercentTypes._80_)
                    .build();
            long startTime = System.currentTimeMillis();
            // Wait for the task to be completed
            AddressesResults addressesResults = balancerManager.runScanner();
            System.out.println("Address Results:" + addressesResults);
            System.out.println("Program Completed !!");

            long endTime = System.currentTimeMillis();
            System.out.printf("AFTER ---- VM free memory: %.2f GB of %.2f GB \n",
                    Utils.getMemory(MemoryTypes.FREE_VM_MEMORY, MemorySizeTypes.GB),
                    Utils.getMemory(MemoryTypes.TOTAL_VM_MEMORY, MemorySizeTypes.GB));
            System.out.printf("AFTER ---- SYSTEM free memory: %.2f GB of %.2f GB \n",
                    Utils.getMemory(MemoryTypes.FREE_SYSTEM_MEMORY, MemorySizeTypes.GB),
                    Utils.getMemory(MemoryTypes.TOTAL_SYSTEM_MEMORY, MemorySizeTypes.GB));
            System.out.println("Spend time: " + Utils.convertToTime((endTime - startTime) / 1000));
        }
    }
}