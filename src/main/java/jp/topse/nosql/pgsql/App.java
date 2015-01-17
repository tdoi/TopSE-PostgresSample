package jp.topse.nosql.pgsql;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.postgresql.util.PGobject;

/**
 * Hello world!
 *
 */
public class App {
    public static final String DRIVER = "org.postgresql.Driver";
    public static final String DB_SERVER = "157.1.206.2";
    public static final String DB_NAME = "topse";
    public static final String DB_USER = "topse";
    public static final String DB_PASS = "topse";
    public static final String TABLE_NAME_1 = "logs_test_1";
    public static final String TABLE_NAME_2 = "logs_test_2";

    public static void main(String[] args) {
        Connection con = createConnection();
        if (con == null) {
            return;
        }
        testHstore(con);
        testJson(con);
    }
    
    public static void testHstore(Connection con) {
        try {
            Statement statement = con.createStatement();
            String query = "INSERT INTO " + TABLE_NAME_1 + " (datetime, attributes) " + " VALUES " + " (NOW()," + " 'target => \"index.html\","
                    + "  referer => \"http://google.com\"," + "  parameter => \"x=1\"');";
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement statement = con.prepareStatement("SELECT * FROM " + TABLE_NAME_1);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = (HashMap<String, Object>) resultSet.getObject(3);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + " : " + entry.getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void testJson(Connection con) {
        try {
            Statement statement = con.createStatement();
            String query = "INSERT INTO " + TABLE_NAME_2 + " (datetime, attributes) " + " VALUES " + " (NOW()," +
                    " '{\"target\": \"index.html\","
                    + " \"referer\": \"http://google.com\","
                    + " \"parameter\": {\"x\": 1} }' )";
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement statement = con.prepareStatement("SELECT * FROM " + TABLE_NAME_2);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                PGobject object = (PGobject) resultSet.getObject(3);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> map = mapper.readValue(object.getValue(), Map.class);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + " : " + entry.getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection createConnection() {
        try {
            String url = "jdbc:postgresql://" + DB_SERVER + "/" + DB_NAME;
            Class.forName(DRIVER);
            return DriverManager.getConnection(url, DB_USER, DB_PASS);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static void readAndDo(String path) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(path))));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                
                // ここで各行に対して何かやる
                System.out.println("*****" + line + "*****");
                
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
