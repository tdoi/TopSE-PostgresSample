package jp.topse.nosql.pgsql;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class App {
    public static final String DRIVER = "org.postgresql.Driver";
    public static final String DB_SERVER = "localhost:15432";
    public static final String DB_NAME = "topseXX";
    public static final String DB_USER = "topseXX";
    public static final String DB_PASS = "topse";
    public static final String TABLE_NAME_1 = "logs";
    public static final String TABLE_NAME_2 = "logs2";
    public static final String ACCESS_LOG = "src/main/resources/access.log";

    public static void main(String[] args) {
        Connection con = createConnection();
        if (con == null) {
            return;
        }
//        insertIntoHstoreSample(con);
//        readFromHstoreSample(con);
//        insertIntoJsonSample(con);
//        readFromJsonSample(con);
        
        // clear data
        clearData(con);
        
        // Problem 1
        insertAccessLog(con);

        // Problem 2 and 3
        countData(con);
        
        // Problem 4
        appendWarning(con);
    }
    
    public static void insertIntoHstoreSample(Connection con) {
        try {
            Statement statement = con.createStatement();
            String query = "INSERT INTO " + TABLE_NAME_1
            		+ " (datetime, attributes) "
            		+ " VALUES "
            		+ " (NOW(),"
            			+ "'"
            			+ " target => \"index.html\","
            			+ " referer => \"http://google.com\","
            			+ " parameter => \"x=1\""
            			+ "'"
            			+ ");";
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void readFromHstoreSample(Connection con) {
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

    public static void insertIntoJsonSample(Connection con) {
        try {
            Statement statement = con.createStatement();
            String query = "INSERT INTO " + TABLE_NAME_2
            		+ " (datetime, attributes) " 
            		+ " VALUES "
            		+ " (NOW(),"
            			+ "'"
            			+ "{\"target\": \"index.html\","
            			+ " \"referer\": \"http://google.com\","
            			+ " \"parameter\": {\"x\": 1} }"
            			+ "'"
            			+ ")";
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void readFromJsonSample(Connection con) {
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
    
    public static void clearData(Connection con) {
        try {
            PreparedStatement statement1 = con.prepareStatement("DELETE FROM " + TABLE_NAME_1);
            statement1.executeQuery();
            PreparedStatement statement2 = con.prepareStatement("DELETE FROM " + TABLE_NAME_2);
            statement2.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void insertAccessLog(Connection con) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(ACCESS_LOG))));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                String[] logItems = line.split(",");
                String target = logItems[0];
                Map<String, String> params = new HashMap<String, String>();
                String referer = logItems.length > 1 ? logItems[1] : null;
                String[] targetItems = target.split("\\?");
                if (targetItems.length == 2) {
                	target = targetItems[0];
                	String[] paramItems = targetItems[1].split("&");
                	for (int i = 0; i < paramItems.length; ++i) {
                		String[] items = paramItems[i].split("=");
                		params.put(items[0],  items[1]);
                	}
                }
                
                System.out.println("*****************");
                System.out.println(line);
                System.out.println("Target: "  + target);
                System.out.println("Referer: " + referer);
                System.out.println("Params: "  + params.size());
                for (Map.Entry<String, String> entry : params.entrySet()) {
                	System.out.println("\t" + entry.getKey() + "=" + entry.getValue());
                }
                
                // PLEASE IMPLEMENT HERE

                		
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
    
    public static void countData(Connection con) {
        // PLEASE IMPLEMENT HERE

    }
    
    public static void appendWarning(Connection con) {
        // PLEASE IMPLEMENT HERE
    	
    }

}
