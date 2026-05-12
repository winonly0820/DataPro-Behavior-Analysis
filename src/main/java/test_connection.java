/**
 * 功能：
 * 作者：大神
 * 日期： 2026/3/25 23:11
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class test_connection {

    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://192.168.86.101:10000/datapro_primary";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private static final String TABLE_NAME = "datapro_primary.dashboard_core_label_distribution_59";

    public static void main(String[] args) {
        try {
            Class.forName(DRIVER);

            try (
                    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                    Statement statement = connection.createStatement()
            ) {
                System.out.println("Hive 连接成功！");
                System.out.println();

                // 1. 查看表结构
                printQuery(statement, "DESC " + TABLE_NAME);

                // 2. 查看前 10 行样例数据
                printQuery(statement, "SELECT * FROM " + TABLE_NAME + " LIMIT 10");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printQuery(Statement statement, String sql) throws Exception {
        System.out.println("========== 执行 SQL ==========");
        System.out.println(sql);
        System.out.println();

        try (ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 打印字段名
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i));
                if (i < columnCount) {
                    System.out.print("\t");
                }
            }
            System.out.println();

            // 打印数据
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = resultSet.getString(i);
                    System.out.print(value == null ? "NULL" : value);
                    if (i < columnCount) {
                        System.out.print("\t");
                    }
                }
                System.out.println();
            }
        }

        System.out.println();
    }
}