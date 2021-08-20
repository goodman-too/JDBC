package DButils;

import java.sql.*;

public class JDBCController {
    private static final String URL = "jdbc:mysql://localhost:3306/sakila";
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    private static Connection connection = null;
    private static Statement statement = null;
    private static ResultSet resultSet = null;

    public static Connection connectToDB() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void executeSQL(String query) {
        try {
            statement = connectToDB().createStatement();
            statement.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static ResultSet selectFromTable(String query) {
        try {
            statement = connectToDB().createStatement();
            resultSet = statement.executeQuery(query);
            resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    public static void dropDefaultTableAfterTest() {
        try {
            statement = connectToDB().createStatement();
            statement.execute("DROP TABLE test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createDefaultTable() {
        String query = "CREATE TABLE test ("
                + "ID int(6) NOT NULL,"
                + "FIRST_NAME VARCHAR(45) NOT NULL,"
                + "LAST_NAME VARCHAR(45) NOT NULL,"
                + "TOWN VARCHAR(45),"
                + "PRIMARY KEY (id))";
        executeSQL(query);
    }

    public static String showTableLike(String tableTitle) {
        String query = "SHOW TABLES LIKE ?";
        String result = null;
        try {
            PreparedStatement prepareStatement = connectToDB().prepareStatement(query);
            prepareStatement.setNString(1, tableTitle);
            resultSet = prepareStatement.executeQuery();
            resultSet.next();
            if (resultSet.getRow() > 0) {
                result = resultSet.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    //можно сделать multiLineExecuteSQL(String query, String delimiter)
    public static void createTablesForSelectJoinTest() {
        String query = "CREATE TABLE trucks (number int UNIQUE, model varchar(20), cargo_id int UNIQUE);\n" +
                "INSERT INTO trucks (number, model, cargo_id) VALUES (7456, 'Volvo FH16', 113);\n" +
                "INSERT INTO trucks (number, model, cargo_id) VALUES (4158, 'Mercedes-Benz Actros', 127);\n" +
                "INSERT INTO trucks (number, model, cargo_id) VALUES (9371, 'MAZ-5440', 143);\n" +
                "CREATE TABLE cargos (id int unique, title varchar(40), weight int);\n" +
                "INSERT INTO cargos (id, title, weight) VALUES (100, 'Wood', 16400);\n" +
                "INSERT INTO cargos (id, title, weight) VALUES (127, 'Foam', 3770);\n" +
                "INSERT INTO cargos (id, title, weight) VALUES (143, 'Equipment', 12750);";

        String[] queries = query.split("\n");

        for (String line : queries) {
            JDBCController.executeSQL(line);
        }
    }
}
