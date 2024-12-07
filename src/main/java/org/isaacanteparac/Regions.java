package org.isaacanteparac;

public enum Regions {
    DURAN("Duran"),
    SAMBORONDON("Samborondon");

    private final String name;

    Regions(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
