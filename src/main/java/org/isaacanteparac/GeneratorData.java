package org.isaacanteparac;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class GeneratorData {

    private final Map<String, double[]> coordRanges;
    private final Random random;
    private final ObjectMapper objectMapper;
    private final double[] consumptionRange;

    // Constructor de la clase que inicializa los valores
    public GeneratorData() {
        this.coordRanges = new HashMap<>();
        this.random = new Random();
        this.objectMapper = new ObjectMapper();
        this.consumptionRange = new double[]{0.5, 100};  // Rango de consumo

        // Definir coordenadas para cada región
        this.coordRanges.put(Regions.SAMBORONDON.getName(), new double[]{-1.9, -1.85, -79.9, -79.85});
        this.coordRanges.put(Regions.DURAN.getName(), new double[]{-2.1, -2.05, -79.9, -79.85});
    }

    // Método para generar datos de electricidad en tiempo real
    public String generateElectricityData(String meterId, String region) throws Exception {
        // Seleccionar región aleatoria
        //final String region = randomRegion()
        double[] range = coordRanges.get(region);

        // Generar coordenadas dentro del rango
        double latitude = round(randomInRange(range[0], range[1]), 6);
        double longitude = round(randomInRange(range[2], range[3]), 6);

        // Generar consumo eléctrico
        double consumption = (random.nextDouble() < Double.parseDouble(Config.ANOMALY_KW.getString()))
                ? randomInRange(100, 500) // Consumo atípico
                : randomInRange(consumptionRange[0], consumptionRange[1]);

        // Crear el objeto de datos
        Map<String, Object> data = new HashMap<>();
        data.put("id_medidor", meterId);
        data.put("consumo_kWh", round(consumption, 2));
        data.put("región", region);
        data.put("latitud", latitude);
        data.put("longitud", longitude);
        data.put("timestamp", new Date().toString());

        // Convertir a JSON y retornar
        return objectMapper.writeValueAsString(data);
    }

    // Selecciona una región aleatoria
    private String randomRegion() {
        List<String> regions = new ArrayList<>(coordRanges.keySet());
        return regions.get(random.nextInt(regions.size()));
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
}
