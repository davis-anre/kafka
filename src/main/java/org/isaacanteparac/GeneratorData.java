package org.isaacanteparac;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class GeneratorData {

    private final Map<Regions, double[]> coordRanges;
    private final Map<Regions, List<String>> generatedIds;
    private final Random random;

    private final double[] consumptionRange;

    // Constructor de la clase que inicializa los valores
    public GeneratorData() {
        this.coordRanges = new HashMap<>();
        this.generatedIds = new HashMap<>();
        this.random = new Random();
        this.consumptionRange = new double[]{0.5, 100}; // Rango de consumo

        // Definir coordenadas para cada región
        this.coordRanges.put(Regions.SAMBORONDON, new double[]{-1.9, -1.85, -79.9, -79.85});
        this.coordRanges.put(Regions.DURAN, new double[]{-2.1, -2.05, -79.9, -79.85});

        // Generar las coordenadas y unirlas en IDs para las regiones
        preGenerateIds();
    }

    // Método para generar las coordenadas y sus IDs previamente
    private void preGenerateIds() {
        coordRanges.forEach((region, range) -> {
            List<String> idsList = new ArrayList<>();
            for (int i = 0; i < Integer.parseInt(Config.AMOUNT_CONSUMPTION.getString()); i++) {
                double latitude = round(randomInRange(range[0], range[1]), 6);
                double longitude = round(randomInRange(range[2], range[3]), 6);
                String id = ids(latitude, longitude, region);
                idsList.add(id);
            }
            generatedIds.put(region, idsList);
        });
    }

    // Método para generar datos de electricidad en tiempo real
    public electricalConsumption generateElectricityData(Regions region) throws Exception {
        // Seleccionar un ID aleatorio de la región
        List<String> ids = generatedIds.get(region);
        String selectedId = ids.get(random.nextInt(ids.size()));

        // Separar el ID en latitud y longitud
        String[] parts = selectedId.split("l4t|l0n");
        double latitude = Double.parseDouble(parts[0]);
        double longitude = Double.parseDouble(parts[1]);

        // Generar consumo eléctrico
        double consumption = (random.nextDouble() < Double.parseDouble(Config.ANOMALY_KW.getString()))
                ? randomInRange(100, 500) // Consumo atípico
                : randomInRange(consumptionRange[0], consumptionRange[1]);

        final electricalConsumption data = new electricalConsumption(selectedId, round(consumption, 2),
                new Date().toString(), latitude, longitude,region);

        return data;
    }

    // Genera un número aleatorio dentro de un rango dado
    private double randomInRange(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    // Redondea un valor a un número específico de decimales
    private double round(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    // Une latitud, longitud y región en un ID único
    private String ids(double lat, double lon, Regions region) {
        return lat + "l4t" + lon + "l0n" + region.getName().toLowerCase();
    }
}
