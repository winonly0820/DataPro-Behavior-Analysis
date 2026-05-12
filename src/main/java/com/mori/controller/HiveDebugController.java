package com.mori.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class HiveDebugController {

    private final JdbcTemplate jdbcTemplate;

    public HiveDebugController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/api/hive/tables")
    public List<Map<String, Object>> showTables() {
        return jdbcTemplate.queryForList("SHOW TABLES");
    }

    @GetMapping("/api/dashboard/label/sample")
    public List<Map<String, Object>> labelSample() {
        String sql = "SELECT parent_label, label, user_count " +
                "FROM dashboard_core_label_distribution_59 " +
                "LIMIT 10";

        return jdbcTemplate.queryForList(sql);
    }
}