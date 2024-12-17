package com.lightspeed.tasks.libs.types;

public enum MemorySizeTypes {
    BY(0),KB(10),MB(20),GB(30),TB(40);
    private final int value;
    MemorySizeTypes(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
