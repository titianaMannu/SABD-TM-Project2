package utils;

public enum ShipType {
    MILITARY_SHIP, PASSENGERS_SHIP, CARGO_SHIP, OTHER_SHIP;

    public static ShipType valueOf(int value) {
        if (value == 35) {
            return MILITARY_SHIP;
        } else if (value >= 60 && value <= 69) {
            return PASSENGERS_SHIP;
        } else if (value >= 70 && value <= 79) {
            return CARGO_SHIP;
        } else {
            return OTHER_SHIP;
        }
    }
}
