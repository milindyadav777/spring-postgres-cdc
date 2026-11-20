package dev.practice.cdc;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
class OrderQuery {
    record Order(long id, long revision, String status) {}
    private final JdbcTemplate sink;

    OrderQuery(JdbcTemplate sink) {
        this.sink = sink;
    }

    @GetMapping("/orders/{id}")
    Order find(@PathVariable long id) {
        return sink.query("SELECT id, revision, status FROM order_view WHERE id = ? AND NOT deleted",
                (rs, n) -> new Order(rs.getLong(1), rs.getLong(2), rs.getString(3)), id)
                .stream().findFirst().orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }
}
