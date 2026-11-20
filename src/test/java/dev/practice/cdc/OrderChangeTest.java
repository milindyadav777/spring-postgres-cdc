package dev.practice.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class OrderChangeTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(strings = {"r","c","u","d"})
    void selectsCorrectImage(String op) throws Exception {
        String field = op.equals("d") ? "before" : "after";
        var json = mapper.createObjectNode().put("op", op);
        json.putObject(field).put("id", 4).put("revision", 8).put("status", "PAID");
        var result = OrderChange.parse(json);
        assertEquals(new OrderChange(4, 8, op.equals("d") ? null : "PAID", op.equals("d")), result);
    }

    @Test
    void missingVersionCannotSilentlyBecomeZero() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> OrderChange.parse(
            mapper.readTree("{\"op\":\"u\",\"after\":{\"id\":1,\"status\":\"PAID\"}}")));
    }
}
