package org.isaacanteparac;

public enum Config {
    ID_CONFIG("kafka_examen"),
    IP("192.168.1.14"),
    PORT("9092"),
    BUILDER("builder"),
    SOURCE("source"),
    DURATION_MINUTES("10"),
    LATENCY_MILLIS("1000"),
    ANOMALY_KW("0.25"),//25%
    AMOUNT_CONSUMPTION("500");


    private final String getString;

    Config(String getString) {
        this.getString = getString;
    }

    public String getString() {
        return getString;
    }

}
