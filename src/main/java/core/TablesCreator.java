package core;

import java.sql.*;
import java.util.*;

public class TablesCreator {
    private String url = "jdbc:postgresql://localhost:5432/";
    private static final String user = "postgres";
    private static final String pass = "postgres";

    private static final String[] companies = {"Aeroflot", "British Airways"};
    private static final String[] countries = {"Russia", "USA", "Germany", "France", "China", "Japan", "Egypt", "Thailand", "Brazil", "Mexico", "Argentina"};
    private static final String[] airbuses = {"Boeing-747", "Boeing-757", "Airbus-380", "IL-96", "TU-154"};
    private static final String[] firstNames = {"Иван", "Сергей", "Александр", "Владимир", "Петр", "Дмитрий", "Юрий", "Григорий", "Василий"};
    private static final String[] lastNames = {"Иванов", "Петров", "Волков", "Медведев", "Ли", "Сухов", "Вавилов", "Григорьев", "Васильев"};


    public TablesCreator() {
    }

    public TablesCreator(String DBName) {
        this.url += DBName;
    }

    public void createTables() {
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Statement statement = conn.createStatement();
            statement.execute("CREATE TABLE companies (id INTEGER PRIMARY KEY, name VARCHAR(64));");
            statement.execute("CREATE TABLE countries (id INTEGER PRIMARY KEY, name VARCHAR(64));");
            statement.execute("CREATE TABLE airbuses (id  INTEGER PRIMARY KEY,model VARCHAR(64), company_id INTEGER, CONSTRAINT airbuses_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies(id));");
            //Fill companies table
            fillTable(statement, "companies", companies);
            //Fill countries table
            fillTable(statement, "countries", countries);
            //Fill airbuses table
            fillTable(statement, "airbuses", 10, airbuses, "companies");
            //Create pilots table
            createPilotsTable(statement);
            //Fill pilots table
            fillPilotsTableWithRandomUsers(statement, 20, firstNames, lastNames);
            //Create routes routes
            createRoutesTable(statement);
            //Fill routes table
            fillRoutesTable(statement, "routes", 10, "countries");
            //Create companiesRoutesAirbuses table
            createCompaniesRoutesBusesTable(statement);
            //Fill companiesRoutesAirbuses table
            fillCompaniesRoutesAirbusesTable(statement, "companies_routes_airbuses", "routes", "companies", "airbuses");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createPilotsTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE pilots (pilot_id INTEGER PRIMARY KEY,firstname VARCHAR(64), lastname VARCHAR(64), company_id INTEGER," +
                "CONSTRAINT pilots_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id));");
    }

    private void createRoutesTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE routes (id INTEGER PRIMARY KEY, from_country INTEGER, to_country INTEGER," +
                " CONSTRAINT route_from_fk FOREIGN KEY (from_country) REFERENCES countries (id)," +
                " CONSTRAINT route_to_fk FOREIGN KEY (to_country) REFERENCES countries (id));");
    }

    private void createCompaniesRoutesBusesTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE companies_routes_airbuses (union_id INTEGER PRIMARY KEY, company_id  INTEGER, route_id INTEGER, airbus_id INTEGER, " +
                "CONSTRAINT companies_buses_routes_to_routes_fk FOREIGN KEY (route_id) REFERENCES routes (id), " +
                "CONSTRAINT companies_buses_routes_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id)," +
                "CONSTRAINT companies_buses_routes_to_airbuses_fk FOREIGN KEY (airbus_id) REFERENCES airbuses (id));");
    }

    private void fillRoutesTable(Statement statement, String fillingTable, int rowsCount, String countries) throws SQLException {
        int[] resultsArray = getIntValuesArrayFromColumn(statement, countries, "id");
        int[] arr1 = randomValuesArrayFromArray(resultsArray, rowsCount >> 1, 1);
        int[] arr2 = randomValuesArrayFromArray(Arrays.copyOfRange(resultsArray, 1, resultsArray.length), rowsCount - arr1.length, 2);


        for (int i = 0; i < arr1.length; i++) {
            statement.executeUpdate("INSERT INTO " + fillingTable + " VALUES (" + (i + 1) + "," + 1 + "," + arr1[i] + ");");
        }
        for (int i = 0; i < arr2.length; i++) {
            statement.executeUpdate("INSERT INTO " + fillingTable + " VALUES (" + (i + arr1.length + 1) + "," + 2 + "," + arr2[i] + ");");
        }
    }

    private void fillCompaniesRoutesAirbusesTable(Statement statement, String table, String routes, String companies, String airbuses) throws SQLException {
        int[] routesIds = getIntValuesArrayFromColumn(statement, routes, "id");
        int[] companiesIds = getIntValuesArrayFromColumn(statement, companies, "id");

        for (int currentCompany : companiesIds) {
            int[] routesForCompany = randomValuesArrayFromArray(routesIds);
            ResultSet rs = statement.executeQuery("SELECT id FROM " + airbuses + " WHERE company_id = " + currentCompany + ";");
            ArrayList<Integer> result = new ArrayList<>();
            while (rs.next()) {
                result.add(rs.getInt("id"));
            }
            int[] buses = result.stream().mapToInt(i -> i).toArray();

            for (int currentRoute : routesForCompany) {
                int[] companyBuses = randomValuesArrayFromArray(buses);

                for (int currentBus : buses) {
                    int id = Integer.parseInt(Integer.toString(currentRoute) + Integer.toString(currentCompany) + Integer.toString(currentBus));
                    statement.execute("INSERT INTO " + table + " VALUES (" + id + ", " + currentCompany + ", " + currentRoute + ", " + currentBus + ")");
                }
            }
        }
    }

    private void fillPilotsTableWithRandomUsers(Statement stmt, int recordCount, String[] firstnames, String[] lastnames) throws SQLException {
        int[] companies = getIntValuesArrayFromColumn(stmt, "companies", "id");
        for (int i = 0; i < recordCount; i++) {
            stmt.execute("INSERT INTO pilots VALUES (" + (i + 1) + ", '" +
                    Utils.getRandonString(firstnames) + "','" +
                    Utils.getRandonString(lastnames) + "'," +
                    Utils.getRandomInt(companies) + ");");
        }
    }

    private void fillTable(Statement stmt, String table, String[] values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            stmt.executeUpdate("INSERT INTO " + table + " VALUES (" + (i + 1) + ", '" + values[i] + "');");
        }
    }

    private void fillTable(Statement statement, String table, int rows, String[] buses, String refTable) throws SQLException {
        int[] resultsArray = getIntValuesArrayFromColumn(statement, refTable, "id");
        int[] arrayWithValues = Utils.getArrayOfRandomValues(resultsArray, rows);
        for (int i = 0; i < rows; i++) {
            statement.execute("INSERT INTO " + table + " VALUES (" + (i + 1) + ", '" + Utils.getRandonString(buses) + "', " + arrayWithValues[i] + ");");
        }
    }

    public void deleteAllTables() {
        ArrayList<String> tables = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, "public", null, new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
            if (tables.size() != 0) {
                Statement statement = conn.createStatement();
                for (String table : tables) {
                    statement.execute("DROP TABLE IF EXISTS " + table + " CASCADE ;");
                }
            } else {
                throw new NullPointerException("FIND NO TABLES");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String[] showTables() {
        ArrayList<String> tables = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, "public", null, new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (tables.size() != 0) {
            return tables.toArray(new String[tables.size()]);
        } else {
            throw new NullPointerException("FIND NO TABLES");
        }
    }

    private int[] getIntValuesArrayFromColumn(Statement statement, String table, String column) throws SQLException {
        ResultSet set = statement.executeQuery("SELECT " + column + " FROM " + table + ";");
        ArrayList<Integer> result = new ArrayList<>();
        while (set.next()) {
            result.add(set.getInt(column));
        }
        return result.stream().mapToInt(i -> i).toArray();
    }

    private int[] randomValuesArrayFromArray(int[] arr) {
        int count = new Random().nextInt(arr.length);
        while (count <= arr.length >> 1) {
            count = new Random().nextInt(arr.length);
        }
        return randomValuesArrayFromArray(arr, count);
    }

    private int[] randomValuesArrayFromArray(int[] arr, int count) {
        if (count < arr.length) {
            Set<Integer> set = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                int randNumber = new Random().nextInt(arr.length);
                boolean flag = true;
                while (flag) {
                    flag = set.add(arr[randNumber]);
                }
            }
            return set.stream().mapToInt(i -> i).toArray();
        } else
            return arr;
    }

    private int[] randomValuesArrayFromArray(int[] arr, int count, int ignore) {
        if (count < arr.length) {
            Set<Integer> set = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                int randNumber = ignore;
                while (randNumber == ignore) {
                    randNumber = new Random().nextInt(arr.length);
                }
                boolean flag = true;
                while (flag) {
                    flag = set.add(arr[randNumber]);
                }
            }
            return set.stream().mapToInt(i -> i).toArray();
        } else
            return arr;
    }
}

