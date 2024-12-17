package com.lightspeed.tasks.libs.types;

public enum PercentTypes {
    _10_(10),_20_(20),_30_(30),_40_(40),_50_(50),
    _60_(60),_70_(70),_80_(80),_90_(90),_100_(100);
    final int value;

    PercentTypes(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static PercentTypes fromValue(int value) {
        for (PercentTypes type : PercentTypes.values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }
}
