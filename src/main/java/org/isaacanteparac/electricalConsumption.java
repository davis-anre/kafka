package org.isaacanteparac;

public record electricalConsumption(
        String id_medidor,
        double consumption_kWh,
        String timestamp,
        double latitud,
        double longitud,
        Regions region) {
}
