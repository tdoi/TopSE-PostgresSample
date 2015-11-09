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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.postgresql.util.PGobject;

// CREATE TABLE logs (
//	    id SERIAL PRIMARY KEY,
//	    datetime TIMESTAMP,
//	    attributes hstore
//	);
// CREATE TABLE logs2 (
//	    id SERIAL PRIMARY KEY,
//	    datetime TIMESTAMP,
//	    attributes json
//	);
// CREATE TABLE logs3 (
//	    id SERIAL PRIMARY KEY,
//	    datetime TIMESTAMP,
//	    attributes jsonb
//	);
//CREATE TABLE logs4 (
//	    id SERIAL PRIMARY KEY,
//	    datetime TIMESTAMP,
//);
//CREATE TABLE logs4_attributes (
//		logs4_id INTEGER NOT NULL,
//		name     VARCHAR(32) NOT NULL,
//		value    VARCHAR(255) NOT NULL
//);


public class App {
    public static final String DRIVER = "org.postgresql.Driver";
    public static final String DB_SERVER = "localhost:5432";
    public static final String DB_NAME = "topse";
    public static final String DB_USER = "doi";
    public static final String DB_PASS = "";
    public static final String TABLE_NAME_1 = "logs";
    public static final String TABLE_NAME_2 = "logs2";
    public static final String TABLE_NAME_3 = "logs3";
    public static final String TABLE_NAME_4 = "logs4";
    public static final String TABLE_NAME_4_EXTRA = "logs4_attributes";
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
        	Statement statement = con.createStatement();
        	statement.execute("DELETE FROM " + TABLE_NAME_1);
        	statement.execute("DELETE FROM " + TABLE_NAME_2);
        	statement.execute("DELETE FROM " + TABLE_NAME_3);
        	statement.execute("DELETE FROM " + TABLE_NAME_4);
        	statement.execute("DELETE FROM " + TABLE_NAME_4_EXTRA);
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

                // hstore
                Map<String, String> hstoreMap = makeHstoreMap(target, referer, params);
                PreparedStatement hstoreStatement = con.prepareStatement("INSERT INTO " + TABLE_NAME_1 + " (datetime, attributes) VALUES (NOW(), ?)");
                hstoreStatement.setObject(1, hstoreMap);
                hstoreStatement.executeUpdate();

                // json
                PGobject json = new PGobject();
                json.setType("json");
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> jsonMap = makeJsonMap(target, referer, params);
                json.setValue(mapper.writeValueAsString(jsonMap));
                PreparedStatement jsonStatement = con.prepareStatement("INSERT INTO " + TABLE_NAME_2 + " (datetime, attributes) VALUES (NOW(), ?)");
                jsonStatement.setObject(1, json);
                jsonStatement.executeUpdate();

                // jsonb
                PGobject jsonb = new PGobject();
                jsonb.setType("jsonb");
                jsonb.setValue(mapper.writeValueAsString(jsonMap));
                PreparedStatement jsonbStatement = con.prepareStatement("INSERT INTO " + TABLE_NAME_3 + " (datetime, attributes) VALUES (NOW(), ?)");
                jsonbStatement.setObject(1, jsonb);
                jsonbStatement.executeUpdate();
                
                // EAV
                insertEAVRecords(con, target, referer, params);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
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
    
    private static Map<String, String> makeHstoreMap(String target, String referer, Map<String, String> params) {
    	Map<String, String> map = new HashMap<String, String>();
    	map.put("target", target);
    	if (referer != null) {
    		map.put("referer", referer);
    	}
    	List<String> paramsList = new LinkedList<String>();
    	for (Map.Entry<String, String> entry : params.entrySet()) {
    		paramsList.add(entry.getKey() + "=" + entry.getValue());
    	}
    	if (paramsList.size() > 0) {
    		map.put("params", String.join("&", paramsList));
    	}
    	
    	return map;
    }

    private static Map<String, Object> makeJsonMap(String target, String referer, Map<String, String> params) {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put("target", target);
    	if (referer != null) {
    		map.put("referer", referer);
    	}
    	if (params.size() > 0) {
    		map.put("params", params);
    	}
    	
    	return map;
    }
    
    private static void insertEAVRecords(Connection con, String target, String referer, Map<String, String> params) {
    	try {
    		Statement statement = con.createStatement();
			statement.execute("INSERT INTO " + TABLE_NAME_4  + " (datetime) VALUES (NOW())", Statement.RETURN_GENERATED_KEYS);
			ResultSet resultSet = statement.getGeneratedKeys();
			resultSet.next();
			int id = resultSet.getInt(1);
			
			insertEAVValue(con, id, "target", target);
			if (referer != null) {
				insertEAVValue(con, id, "referer", referer);
			}
			for (Map.Entry<String, String> entry : params.entrySet()) {
				insertEAVValue(con, id, entry.getKey(), entry.getValue());
			}			
		} catch (SQLException e) {
			e.printStackTrace();
		} 
    }
    
    private static void insertEAVValue(Connection con, int id, String key, String value) throws SQLException {
    	PreparedStatement statement = con.prepareStatement("INSERT INTO " + TABLE_NAME_4_EXTRA + " VALUES (?, ?, ?)");
    	statement.setInt(1,  id);
    	statement.setString(2, key);
    	statement.setString(3,  value);
    	statement.executeUpdate();
    }

    public static void countData(Connection con) {
        // PLEASE IMPLEMENT HERE
    	
    	// Problem2 for hstore
        try {
        	String query = "SELECT COUNT(*) FROM " + TABLE_NAME_1 + " WHERE attributes->'target' = 'index.html'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_1 + " contains " + count + " records ( target = index.html )");
		} catch (SQLException e) {
			e.printStackTrace();
		}

    	// Problem2 for json
        try {
        	String query = "SELECT COUNT(*) FROM " + TABLE_NAME_2 + " WHERE attributes->>'target' = 'index.html'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_2 + " contains " + count + " records ( target = index.html ).");
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
        // Problem2 for EAV
        try {
        	String query = "SELECT COUNT(*) FROM " + TABLE_NAME_4 + " INNER JOIN " + TABLE_NAME_4_EXTRA + " ON logs4_id = id WHERE name = 'target' AND value = 'index.html'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_4 +" contains " + count + " records ( target = index.html ).");
		} catch (SQLException e) {
			e.printStackTrace();
		}        

    	// Problem3 for hstore
        try {
        	String query ="SELECT COUNT(*) FROM " + TABLE_NAME_1 + " WHERE attributes->'target' = 'index.html' AND "
        			+ "("
        			+ "attributes->'params' = 'x=1'"
        			+ " OR "
        			+ "attributes->'params' LIKE 'x=1&%'"
        			+ " OR "
        			+ "attributes->'params' LIKE '%&x=1'"
        			+ " OR "
        			+ "attributes->'params' LIKE '%&x=1&%'"
        			+ ")";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_1 + " contains " + count + " records ( target = 1 and x = 1 ).");
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
    	// Problem3 for json
        try {
        	String query = "SELECT COUNT(*) FROM " + TABLE_NAME_2 + " WHERE attributes->>'target' = 'index.html' AND attributes->'params'->>'x' = '1'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_2 + " contains " + count + " records ( target = 1 and x = 1 ).");
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
        // Problem2 for EAV
        try {
        	String query = "SELECT COUNT(*) FROM " + TABLE_NAME_4
        			     + " WHERE "
        			     + " id IN (SELECT logs4_id FROM " + TABLE_NAME_4_EXTRA + " WHERE name = 'target' AND value = 'index.html')"
        				 + " AND "
        				 + " id IN (SELECT logs4_id FROM " + TABLE_NAME_4_EXTRA + " WHERE name = 'x' AND value = '1')";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int count = resultSet.getInt(1);
			System.out.println(TABLE_NAME_4 + " contains " + count + " records ( target = index.html and x = 1 ).");
		} catch (SQLException e) {
			e.printStackTrace();
		}        
    }
    
    public static void appendWarning(Connection con) {
        // PLEASE IMPLEMENT HERE
    
    	// For hstore
    	try {
        	String query = "UPDATE " + TABLE_NAME_1 + " SET attributes = (attributes || 'warning => 1') WHERE attributes->'referer' = 'http://badsite.com'";
			PreparedStatement statement = con.prepareStatement(query);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

    	// For hstore
       	try {
        	String query = "UPDATE " + TABLE_NAME_1 + " SET attributes = (attributes || ?) WHERE attributes->'referer' = 'http://badsite.com'";
			PreparedStatement statement = con.prepareStatement(query);
			Map<String, String> map = new HashMap<String, String>();
			map.put("warning2", "1");
			statement.setObject(1,  map);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

    	// For json
    	try {
        	String query = "SELECT id, attributes FROM " + TABLE_NAME_2 + " WHERE attributes->>'referer' = 'http://badsite.com'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
                PGobject object = (PGobject) resultSet.getObject(2);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> map = mapper.readValue(object.getValue(), Map.class);
                map.put("warning", "1");

                PGobject json = new PGobject();
                json.setType("json");
                json.setValue(mapper.writeValueAsString(map));
                PreparedStatement updateStatement = con.prepareStatement("UPDATE " + TABLE_NAME_2 + " SET attributes = ? WHERE id = ?");
                updateStatement.setObject(1, json);
                updateStatement.setInt(2, id);
                updateStatement.executeUpdate();
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

    	// For EAV
    	try {
        	String query = "SELECT logs4_id FROM " + TABLE_NAME_4_EXTRA + " WHERE name = 'referer' AND value = 'http://badsite.com'";
			PreparedStatement statement = con.prepareStatement(query);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
                PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + TABLE_NAME_4_EXTRA + " VALUES (?, ?, ?)");
                insertStatement.setInt(1, id);
                insertStatement.setString(2, "warning");
                insertStatement.setString(3, "1");
                insertStatement.executeUpdate();
    		}
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

}
