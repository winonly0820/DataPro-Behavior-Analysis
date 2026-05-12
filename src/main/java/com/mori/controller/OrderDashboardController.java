package com.mori.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

@RestController
@CrossOrigin
public class OrderDashboardController {

    @Value("${spring.redis.host}")
    private String redisHost;

    @Value("${spring.redis.port}")
    private int redisPort;

    @GetMapping("/api/order/dashboard")
    public Map<String, Object> getOrderDashboard() {
        try (Jedis jedis = new Jedis(redisHost, redisPort)) {

            Map<String, String> redisData = jedis.hgetAll("mock_order:dashboard");

            Map<String, Object> data = new HashMap<>();
            data.put("totalOrderCount", toLong(redisData.get("total_order_count")));
            data.put("totalSalesAmount", toDouble(redisData.get("total_sales_amount")));
            data.put("totalAvgCost", toDouble(redisData.get("total_avg_cost")));

            data.put("latestBatchId", redisData.getOrDefault("latest_batch_id", ""));
            data.put("latestBatchOrderCount", toLong(redisData.get("latest_batch_order_count")));
            data.put("latestBatchSalesAmount", toDouble(redisData.get("latest_batch_sales_amount")));
            data.put("latestBatchAvgCost", toDouble(redisData.get("latest_batch_avg_cost")));
            data.put("updateTime", redisData.getOrDefault("update_time", ""));

            Map<String, Object> result = new HashMap<>();
            result.put("code", 200);
            result.put("message", "success");
            result.put("data", data);
            return result;

        } catch (Exception e) {
            Map<String, Object> result = new HashMap<>();
            result.put("code", 500);
            result.put("message", "读取 Redis 失败：" + e.getMessage());
            result.put("data", null);
            return result;
        }
    }

    private Long toLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0L;
        }
        return Long.parseLong(value);
    }

    private Double toDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }
}