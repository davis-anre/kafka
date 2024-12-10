package org.isaacanteparac;

public record electricalConsumption(
        String id_medidor,
        double consumo_kWh,
        String timestamp) {

}
