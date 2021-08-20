package tests;

import DButils.JDBCController;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class DBTests {

    @BeforeEach
    public void setUp() {
        Assertions.assertNotNull(JDBCController.connectToDB());
        JDBCController.createDefaultTable();
    }

    @AfterEach
    public void tearDown() {
        JDBCController.dropDefaultTableAfterTest();
        JDBCController.closeConnection();
    }

    @Test
    @DisplayName("Create table test")
    public void createTableTest() {
        //table creates in setUp method
        //check table is exist
        Assertions.assertEquals("test", JDBCController.showTableLike("test"));
    }

    @Test
    @DisplayName("Insert into table test")
    public void insertIntoTableTest() {
        //insert
        String query = "INSERT INTO test (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (412890, 'Leonardo', 'Di Caprio', 'Los Angeles')";
        JDBCController.executeSQL(query);
        //select
        query = "SELECT * FROM test";
        ResultSet resultSet = JDBCController.selectFromTable(query);
        //assert
        assertAll("Should return inserted data",
                () -> assertEquals("412890", resultSet.getString("ID")),
                () -> assertEquals("Leonardo", resultSet.getString("FIRST_NAME")),
                () -> assertEquals("Di Caprio", resultSet.getString("LAST_NAME")),
                () -> assertEquals("Los Angeles", resultSet.getString("TOWN")));
    }

    @Test
    @DisplayName("Select from table test")
    public void selectFromTableTest() throws SQLException {
        //insert data for assert
        String query = "INSERT INTO test (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (412890, 'Leonardo', 'Di Caprio', 'Los Angeles')";
        JDBCController.executeSQL(query);
        //select
        query = "SELECT * FROM test WHERE id=412890";
        ResultSet resultSet = JDBCController.selectFromTable(query);
        String expectedTown = "Los Angeles";
        String actualTown = resultSet.getString("town");
        assertEquals(expectedTown, actualTown);
    }

    @Test
    @DisplayName("Remove from table test")
    public void dropFromTableTest() throws SQLException {
        //insert record to removing
        String query = "INSERT INTO test (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (412890, 'Leonardo', 'Di Caprio', 'Los Angeles')";
        JDBCController.executeSQL(query);
        //remove
        query = "DELETE FROM test WHERE ID=412890";
        JDBCController.executeSQL(query);
        //assert
        query = "SELECT * FROM test";
        ResultSet resultSet = JDBCController.selectFromTable(query);
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Update records test")
    public void updateTest() throws SQLException {
        //insert record to update
        String query = "INSERT INTO test (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (412890, 'Leonardo', 'Di Caprio', 'Los Angeles')";
        JDBCController.executeSQL(query);
        //update
        query = "UPDATE test SET TOWN = 'Moscow' WHERE ID=412890";
        JDBCController.executeSQL(query);
        //assert
        query = "SELECT * FROM test WHERE ID=412890";
        ResultSet resultSet = JDBCController.selectFromTable(query);
        String expectedTown = "Moscow";
        String actualTown = resultSet.getString("town");
        Assertions.assertEquals(expectedTown, actualTown);
    }

    @Test
    @DisplayName("Remove table test")
    public void dropTableTest() {
        //create table to remove
        String query = "CREATE TABLE removeMe (id SERIAL UNIQUE, name varchar(20))";
        JDBCController.executeSQL(query);
        //check table
        Assertions.assertEquals("removeme", JDBCController.showTableLike("removeme"));
        //remove
        query = "DROP TABLE removeme";
        JDBCController.executeSQL(query);
        //assert
        Assertions.assertNull(JDBCController.showTableLike("removeme"));
    }

    @Test
    @DisplayName("Select join test")
    public void selectJoinTest() throws SQLException {
        //create tables with records
        JDBCController.createTablesForSelectJoinTest();

        //select + join
        String query = "SELECT trucks.model, cargos.title, cargos.weight " +
                "FROM trucks " +
                "JOIN cargos " +
                "ON trucks.cargo_id=cargos.id;";
        ResultSet resultSet = JDBCController.selectFromTable(query);

        //asserts first line
        assertAll("Should return selected data",
                () -> assertEquals("Mercedes-Benz Actros", resultSet.getString("model")),
                () -> assertEquals("Foam", resultSet.getString("title")),
                () -> assertEquals(3770 ,resultSet.getInt("weight")));
        resultSet.next();
        //asserts second line
        assertAll("Should return selected data",
                () -> assertEquals("MAZ-5440", resultSet.getString("model")),
                () -> assertEquals("Equipment", resultSet.getString("title")),
                () -> assertEquals(12750 ,resultSet.getInt("weight")));

        //remove tables
        query = "DROP TABLE trucks;";
        JDBCController.executeSQL(query);
        query = "DROP TABLE cargos;";
        JDBCController.executeSQL(query);
    }

    @Test
    @DisplayName("Left join test")
    public void leftJoinTest() {
        //create tables with records
        JDBCController.createTablesForSelectJoinTest();

        //select + join
        String query = "SELECT trucks.model, cargos.title, cargos.weight " +
                "FROM trucks " +
                "LEFT JOIN cargos " +
                "ON trucks.cargo_id=cargos.id;";
        ResultSet resultSet = JDBCController.selectFromTable(query);

        //asserts first line
        assertAll("Should return selected data",
                () -> assertEquals("Volvo FH16", resultSet.getString("model")),
                () -> assertNull(resultSet.getString("title")),
                () -> assertEquals(0 ,resultSet.getInt("weight")));

        //remove tables
        query = "DROP TABLE trucks;";
        JDBCController.executeSQL(query);
        query = "DROP TABLE cargos;";
        JDBCController.executeSQL(query);
    }

    @Test
    @DisplayName("Right join test")
    public void rightJoinTest() {
        //create tables with records
        JDBCController.createTablesForSelectJoinTest();

        //select + join
        String query = "SELECT trucks.model, cargos.title, cargos.weight " +
                "FROM trucks " +
                "RIGHT JOIN cargos " +
                "ON trucks.cargo_id=cargos.id;";
        ResultSet resultSet = JDBCController.selectFromTable(query);

        //asserts first line
        assertAll("Should return selected data",
                () -> assertNull(resultSet.getString("model")),
                () -> assertEquals("Wood" ,resultSet.getString("title")),
                () -> assertEquals(16400 ,resultSet.getInt("weight")));

        //remove tables
        query = "DROP TABLE trucks;";
        JDBCController.executeSQL(query);
        query = "DROP TABLE cargos;";
        JDBCController.executeSQL(query);
    }
}
