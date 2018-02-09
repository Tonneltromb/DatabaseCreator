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

    public String getUrl() {
        return url;
    }

    public static String getUser() {
        return user;
    }

    public static String getPass() {
        return pass;
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

    public void clearTable(String tableName) {
        try(Connection connection = DriverManager.getConnection(url,user,pass)) {
            Statement statement = connection.createStatement();
            statement.execute("DELETE FROM "+tableName+";");
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

    public void createTables() {
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Statement statement = conn.createStatement();
            //Create companies table
            createSimpleTable("companies", statement, true);
            //Create countries table
            createSimpleTable("countries", statement, true);
            //Create airbuses table
            createSimpleTable("airbuses", statement, true);
            //Create pilots table
            createPilotsTable(statement);
            //Create routes routes
            createRoutesTable(statement);
            //Create companiesAirbusesTable
            createCompaniesAirbusesTable(statement);
            //Create companiesRoutesAirbuses table
            createCompaniesRoutesAirbusesTable(statement);
            //Create flights table
            createFlightsTable(statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void fillTables() {
        try (Connection connection = DriverManager.getConnection(url, user, pass)) {
            Statement statement = connection.createStatement();
            //Fill companies table
            fillSimpleTable(statement, "companies", companies);
            //Fill countries table
            fillSimpleTable(statement, "countries", countries);
            //Fill airbuses table
            fillSimpleTable(statement, "airbuses", airbuses, 10, true);
            //Fill pilots table
            fillPilotsTableWithRandomPilots(statement, 20, firstNames, lastNames);
            //Fill routes table
            fillRoutesTable(statement, "routes", 10, 2);
            //fill companiesAirbusesTable
            fillCompaniesAirbusesTable(statement, "companies_airbuses", 10);
            //Fill companiesRoutesAirbuses table
            fillCompaniesRoutesAirbusesTable(statement, "companies_routes_airbuses", 4);
            //Fill flights table
            fillFlightsTable(statement, 10);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createSimpleTable(String tableName, Statement statement, boolean isSerial) throws SQLException {
        String type = "INTEGER";
        if (isSerial) {
            type = "SERIAL";
        }
        statement.execute("CREATE TABLE " + tableName + "(id " + type + " PRIMARY KEY, name VARCHAR(64));");
    }

    public void createSimpleTable(String tableName, Statement statement) throws SQLException {
        createSimpleTable(tableName, statement, false);
    }

    private void createPilotsTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE pilots (id SERIAL PRIMARY KEY,firstname VARCHAR(64), lastname VARCHAR(64), company_id INTEGER," +
                "CONSTRAINT pilots_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id));");
    }

    //todo method creating table wired with other two
    private void createTwoWiredTable(Statement statement, String tableName, String firstfield, String firstTable, String secondField, String secondTable) {
    }

    private void createRoutesTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE routes (id SERIAL PRIMARY KEY, from_country INTEGER, to_country INTEGER," +
                " CONSTRAINT route_from_fk FOREIGN KEY (from_country) REFERENCES countries (id)," +
                " CONSTRAINT route_to_fk FOREIGN KEY (to_country) REFERENCES countries (id));");
    }

    private void createCompaniesAirbusesTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE companies_airbuses ( id SERIAL PRIMARY KEY," +
                "company_id INTEGER," +
                "airbus_id INTEGER," +
                "CONSTRAINT companies_airbuses_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id)," +
                "CONSTRAINT companies_airbuses_to_airbuses_fk FOREIGN KEY (airbus_id) REFERENCES airbuses (id));");
    }

    private void createCompaniesRoutesAirbusesTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE companies_routes_airbuses (" +
                "id SERIAL PRIMARY KEY," +
                "route_id INTEGER," +
                "com_bus_id INTEGER," +
                "CONSTRAINT companies_routes_buses_to_routes_fk FOREIGN KEY (route_id) REFERENCES routes (id)," +
                "CONSTRAINT companies_routes_buses_to_companies_buses_fk FOREIGN KEY (com_bus_id) REFERENCES companies_airbuses (id));");
    }

    private void createFlightsTable(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE flights (" +
                "id SERIAL PRIMARY KEY," +
                "com_rout_bus_id INTEGER," +
                "first_pilot INTEGER," +
                "second_pilot INTEGER," +
                "departure TIMESTAMP," +
                "arriwe TIMESTAMP," +
                "CONSTRAINT flights_to_companies_routes_buses_fk FOREIGN KEY (com_rout_bus_id) REFERENCES companies_routes_airbuses (id)," +
                "CONSTRAINT flights_to_first_pilot_fk FOREIGN KEY (first_pilot) REFERENCES pilots (id),CONSTRAINT flights_to_second_pilot_fk FOREIGN KEY (second_pilot) REFERENCES pilots (id));");
    }

    private void fillCompaniesRoutesAirbusesTable(Statement statement, String tableName, int numberOfRoutesForCompany) throws SQLException {
        int[] routesId = getValuesArrayFromColumn(statement, "routes", "id");
        int[] companiesId = getValuesArrayFromColumn(statement, "companies", "id");
        if (numberOfRoutesForCompany < routesId.length) {
            for (int i = 0; i < companiesId.length; i++) {
                ResultSet set = statement.executeQuery("SELECT id FROM companies_airbuses WHERE company_id = " + companiesId[i] + ";");
                ArrayList<Integer> result = new ArrayList<>();
                while (set.next()) {
                    result.add(set.getInt("id"));
                }
                int[] companyBuses = result.stream().mapToInt(j -> j).toArray();
                int[] originalCompanyBuses = Arrays.copyOfRange(companyBuses, 0, companyBuses.length);
                int[] routes = randomValuesFromArray(routesId, numberOfRoutesForCompany);
                int[] originRoutes = Arrays.copyOfRange(routes, 0, routes.length);
                if (companyBuses.length > routes.length) {
                    for (int j = 0; j < companyBuses.length; j++) {
                        int rt;
                        if (routes.length >= 1) {
                            Utils.Pair pair = Utils.getRandomValueAndArrayWithoutThisValue(routes);
                            routes = pair.getArray();
                            rt = pair.getValue();
                        } else {
                            rt = Utils.getRandomInt(originRoutes);
                        }
                        statement.execute("INSERT INTO " + tableName + " (route_id,com_bus_id) VALUES (" + rt + "," + companyBuses[j] + ");");
                    }
                } else {
                    for (int j = 0; j < routes.length; j++) {
                        Utils.Pair pair = Utils.getRandomValueAndArrayWithoutThisValue(companyBuses);
                        routes = pair.getArray();
                        int cb;
                        companyBuses = pair.getArray();
                        if (companyBuses.length >= 1) {
                            cb = pair.getValue();
                        } else {
                            int randValue = new Random().nextInt(companyBuses.length / numberOfRoutesForCompany) + 1;
                            for (int k = 0; k < randValue; k++) {
                                cb = Utils.getRandomInt(originalCompanyBuses);
                                statement.execute("INSERT INTO " + tableName + " (route_id,com_bus_id) VALUES (" + routes[j] + "," + cb + ");");
                            }
                            continue;
                        }
                        statement.execute("INSERT INTO " + tableName + " (route_id,com_bus_id) VALUES (" + routes[j] + "," + cb + ");");
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Bad value");
        }
    }

    private void fillPilotsTableWithRandomPilots(Statement stmt, int rows, String[] firstnames, String[] lastnames) throws SQLException {
        int[] companies = getValuesArrayFromColumn(stmt, "companies", "id");
        for (int i = 0; i < companies.length; i++) {
            int count = rows / companies.length;
            int company;
            if (i == companies.length - 1) {
                company = companies[i];
                count = rows - (i * count);
            } else {
                company = companies[i];
            }
            for (int j = 0; j < count; j++) {
                stmt.execute("INSERT INTO pilots (firstname, lastname, company_id) VALUES ('" +
                        Utils.getRandomString(firstnames) + "','" + Utils.getRandomString(lastnames) + "'," + company + ");");
            }
        }
    }

    private void fillSimpleTable(Statement stmt, String table, String[] names) throws SQLException {
        for (int i = 0; i < names.length; i++) {
            stmt.executeUpdate("INSERT INTO " + table + " VALUES (" + (i + 1) + ", '" + names[i] + "');");
        }
    }

    private void fillSimpleTable(Statement stmt, String table, String[] names, boolean isRepeatableValues) throws SQLException {
        fillSimpleTable(stmt, table, names, names.length, isRepeatableValues);
    }

    private void fillSimpleTable(Statement stmt, String table, String[] names, int count, boolean isRepeatableNames) throws SQLException {
        if (count > names.length && !isRepeatableNames) {
            throw new IllegalArgumentException("IMPOSSIBLE TO CREATE ARRAY OF NON-REPEATABLE VALUES FROM LESS LENGTH ARRAY");
        }
        if (isRepeatableNames) {
            for (int i = 0; i < count; i++) {
                stmt.execute("INSERT INTO " + table + "(name) VALUES ('" + Utils.getRandomString(names) + "');");
            }
        } else {
            for (int i = 0; i < count; i++) {
                stmt.executeUpdate("INSERT INTO " + table + "(name) VALUES ('" + names[i] + "');");
            }
        }
    }

    private void fillCompaniesAirbusesTable(Statement statement, String tableName, int rows) throws SQLException {
        int[] companiesId = getValuesArrayFromColumn(statement, "companies", "id");
        int[] airbusesId = getValuesArrayFromColumn(statement, "airbuses", "id");
        for (int i = 0; i < companiesId.length; i++) {
            int count = rows / companiesId.length;
            int[] buses;
            if (i == companiesId.length - 1) {
                buses = randomValuesFromArray(airbusesId, rows - (i * count));
            } else {
                buses = randomValuesFromArray(airbusesId, count);
            }
            for (int bus : buses) {
                statement.execute("INSERT INTO " + tableName + "(company_id, airbus_id) VALUES (" + companiesId[i] + ", " + bus + ");");
            }
        }
    }

    private void fillRoutesTable(Statement statement, String tableName, int rows, int countries) throws SQLException {
        int[] countriesId = getValuesArrayFromColumn(statement, "countries", "id");
        for (int i = 0; i < countries; i++) {
            int temp = countriesId[0];
            countriesId = Arrays.stream(countriesId).skip(1).toArray();
            int count = rows / countries;
            int[] array;
            if (i == countriesId.length - 1) {
                array = randomValuesFromArray(countriesId, rows - (i * count));
            } else {
                array = randomValuesFromArray(countriesId, count, temp);
            }
            for (int bus : array) {
                statement.execute("INSERT INTO " + tableName + "(from_country, to_country) VALUES (" + temp + ", " + bus + ");");
            }
        }
    }

    private void fillFlightsTable(Statement statement, int rows) throws SQLException {
        // method randomTime
        int[] companiesId = getValuesArrayFromColumn(statement, "companies", "id");
        for (int i = 0; i < companiesId.length; i++) {
            ResultSet set = statement.executeQuery("SELECT cra.id FROM companies_routes_airbuses cra INNER JOIN companies_airbuses ca ON cra.com_bus_id = ca.id WHERE ca.company_id =" + companiesId[i] + ";");
            int[] craId = resultSetToArray(set, "id");
            set = statement.executeQuery("SELECT id FROM pilots WHERE company_id = " + companiesId[i] + ";");
            int[] pilotsIds = resultSetToArray(set, "id");
            int count = rows / companiesId.length;
            if (i == companiesId.length - 1) {
                count = rows - (i * count);
            }
            for (int j = 0; j < count; j++) {
                int[] pair = getPilotsPair(pilotsIds);
                statement.execute("INSERT INTO flights (com_rout_bus_id, first_pilot, second_pilot, departure, arriwe) " +
                        "VALUES (" + Utils.getRandomInt(craId) + "," + pair[0] + "," + pair[1] + ",'" + getRandomTimestampString(2018) + "','" + getRandomTimestampString(2018) + "');");
            }
        }
    }

    private void fillFlightsTable(Statement statement) throws SQLException {
        fillFlightsTable(statement, 20);
    }


    private int[] getValuesArrayFromColumn(Statement statement, String table, String column) throws SQLException {
        ResultSet set = statement.executeQuery("SELECT " + column + " FROM " + table + ";");
        ArrayList<Integer> result = new ArrayList<>();
        while (set.next()) {
            result.add(set.getInt(column));
        }
        return result.stream().mapToInt(i -> i).toArray();
    }

    private int[] randomValuesFromArray(int[] arr) {
        int count = new Random().nextInt(arr.length);
        while (count <= (arr.length >> 1)) {
            count = new Random().nextInt(arr.length);
        }
        return randomValuesFromArray(arr, count);
    }

    private int[] randomValuesFromArray(int[] arr, int count) {
        Set<Integer> set = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            boolean flag = true;
            while (flag) {
                int randNumber = arr[new Random().nextInt(arr.length)];
                flag = !set.add(randNumber);
            }
        }
        return set.stream().mapToInt(i -> i).toArray();
    }

    private int[] randomValuesFromArray(int[] arr, int count, int ignore) {
        if (count > arr.length) {
            throw new IllegalArgumentException("count must be less than array.length");
        }
        Set<Integer> set = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            boolean flag = true;
            while (flag) {
                int randNumber = ignore;
                while (randNumber == ignore) {
                    randNumber = arr[new Random().nextInt(arr.length)];
                }
                flag = !set.add(randNumber);
            }
        }
        return set.stream().mapToInt(i -> i).toArray();
    }

    private int[] multiCountRandomFromArray(int[] array, int count) {
        int[] newArray = new int[count];
        for (int i = 0; i < count; i++) {
            newArray[i] = Utils.getRandomInt(array);
        }
        return newArray;
    }

    private int[] resultSetToArray(ResultSet resultSet, String columnName) throws SQLException {
        ArrayList<Integer> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getInt(columnName));
        }
        return result.stream().mapToInt(i -> i).toArray();
    }

    private String[] getPilotsPair(String[] pilots) {
        String firstPilot = Utils.getRandomString(pilots);
        String secondPilot = Utils.getRandomString(pilots);
        while (firstPilot.equals(secondPilot)) {
            secondPilot = Utils.getRandomString(pilots);
        }
        return new String[]{firstPilot, secondPilot};
    }

    private int[] getPilotsPair(int[] array) {
        int firstPilot = Utils.getRandomInt(array);
        int secondPilot = Utils.getRandomInt(array);
        while (firstPilot == secondPilot) {
            secondPilot = Utils.getRandomInt(array);
        }
        return new int[]{firstPilot, secondPilot};
    }

    private String getRandomTimestampString() {
        String year = getRandomValue(2017, 2018);
        String month = getRandomValue(1, 12);
        String day = getRandomValue(1, 31);
        String hour = getRandomValue(0, 23);
        String minute = getRandomValue(0, 59);
        String second = getRandomValue(0, 59);
        String ymd = String.join("-", year, month, day);
        String hms = String.join(":", hour, minute, second);
        return ymd + " " + hms;
    }

    private String getRandomTimestampString(int yearValue) {
        String year = Integer.toString(yearValue);
        String month = getRandomValue(1, 12);
        String day = getRandomValue(1, 31);
        String hour = getRandomValue(0, 23);
        String minute = getRandomValue(0, 59);
        String second = getRandomValue(0, 59);
        String ymd = String.join("-", year, month, day);
        String hms = String.join(":", hour, minute, second);
        return ymd + " " + hms;
    }

    private String getRandomValue(int from, int to) {
        int value = new Random().nextInt(to - from) + from;
        if (value < 10) {
            return "0" + Integer.toString(value);
        }
        return Integer.toString(value);
    }
}

