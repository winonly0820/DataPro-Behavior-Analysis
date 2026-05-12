package com.mori.controller;

import com.mori.dto.LabelDistributionDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class DashboardController {

    private final JdbcTemplate jdbcTemplate;

    public DashboardController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 查询所有一级标签
     * 访问地址：
     * http://localhost:8080/api/dashboard/label/parents
     */
    @GetMapping("/api/dashboard/label/parents")
    public List<String> getParentLabels() {
        String sql = "SELECT parent_label " +
                "FROM dashboard_core_label_distribution_59 " +
                "GROUP BY parent_label";

        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString(1));
    }

    /**
     * 根据一级标签查询标签分布
     * 访问示例：
     * http://localhost:8080/api/dashboard/label/distribution?parentLabel=电视消费水平
     */
    @GetMapping("/api/dashboard/label/distribution")
    public List<LabelDistributionDTO> getLabelDistributionByParentLabel(
            @RequestParam("parentLabel") String parentLabel
    ) {
        String sql = "SELECT parent_label, label, user_count " +
                "FROM dashboard_core_label_distribution_59 " +
                "WHERE parent_label = ?";

        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new LabelDistributionDTO(
                        rs.getString("parent_label"),
                        rs.getString("label"),
                        rs.getLong("user_count")
                ), parentLabel
        );
    }

    /**
     * 查询全部标签分布
     * 访问地址：
     * http://localhost:8080/api/dashboard/label/distribution/all
     */
    @GetMapping("/api/dashboard/label/distribution/all")
    public List<LabelDistributionDTO> getAllLabelDistribution() {
        String sql = "SELECT parent_label, label, user_count " +
                "FROM dashboard_core_label_distribution_59";

        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new LabelDistributionDTO(
                        rs.getString("parent_label"),
                        rs.getString("label"),
                        rs.getLong("user_count")
                )
        );
    }
}