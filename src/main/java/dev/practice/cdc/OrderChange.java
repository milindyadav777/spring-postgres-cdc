package dev.practice.cdc;

import com.fasterxml.jackson.databind.JsonNode;

record OrderChange(long id, long version, String status, boolean deleted) {
    static OrderChange parse(JsonNode event) {
        String op = event.path("op").asText();
        boolean deleted = switch (op) {
            case "d" -> true;
            case "r", "c", "u" -> false;
            default -> throw new IllegalArgumentException("Unsupported change operation: " + op);
        };
        JsonNode row = event.path(deleted ? "before" : "after");
        long id = positiveLong(row, "id");
        long version = positiveLong(row, "revision");
        if (!deleted && !row.path("status").isTextual()) {
            throw new IllegalArgumentException("Missing status");
        }
        return new OrderChange(id, version, deleted ? null : row.get("status").textValue(), deleted);
    }

    private static long positiveLong(JsonNode row, String field) {
        JsonNode value = row.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() <= 0) {
            throw new IllegalArgumentException("Expected positive integer " + field);
        }
        return value.longValue();
    }
}
