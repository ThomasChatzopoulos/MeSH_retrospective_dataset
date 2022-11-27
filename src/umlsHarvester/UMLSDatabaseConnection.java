/*
 * #%L
 * UMLSQuery
 * %%
 * Copyright (C) 2012 - 2013 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package umlsHarvester;

// import mysql-connector-java-5.1.44-bin.jar
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * A database-baked implementation of the {@link UMLSQueryExecutor} interface.
 * An instance is obtained by calling the static method {@link #getConnection}
 * with the parameters for accessing the database. Additionally, the caller must
 * pass in the database API type. Once an instance has been obtained, any of the
 * queries defined in the {@link UMLSQueryExecutor} interface may be executed.
 *
 *  Modified to use "com.mysql.jdbc.Driver" by default because "org.arp.javautil.sql.DatabaseAPI" didn't work
 *      Updated 08/2/2018
 */
public class UMLSDatabaseConnection {

    private Connection conn;
    private final String url;
    private final String user;
    private final String password;

    private static void log(Level level, String msg) {
        UMLSUtil.logger().log(level, msg);
    }

    private UMLSDatabaseConnection( String url, String user,
                                    String password) {
//        this.url = "jdbc:mysql://localhost/" + dbname;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Returns a
     * <code>UMLSDatabaseConnection</code> for querying a UMLS database. Callers
     * must specify the location and access information for the database.
     *
     * @param url the location of the database
     * @param user the username to access the database
     * @param password the password that goes with the username to access the
     * database
     * @return a <code>UMLSDatabaseConnection</code> accessed by the specified
     * parameters
     */
    public static UMLSDatabaseConnection getConnection(
            String url, String user, String password) {
        return new UMLSDatabaseConnection(url, user, password);
    }
    /**
     *  Function modified to use "com.mysql.jdbc.Driver" 
     * @throws UMLSQueryException
     */
    private void setupConn() throws UMLSQueryException {
        log(Level.FINE, "Attempting to establish database connection...");
        try {
            // The newInstance() call is a work around for some
            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
        }

        try {
//            conn = DriverManager.getConnection("jdbc:mysql://localhost/"+dbName+"?useSSL=false&user="+user+"&password="+pass);
            conn = DriverManager.getConnection(this.url+"?useSSL=false&user="+this.user+"&password="+this.password);

            // Do something with the Connection
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    private void tearDownConn() throws UMLSQueryException {
        if (conn != null) {
            log(Level.FINE, "Attempting to disconnect from the database...");
            try {
                conn.close();
                log(Level.FINE, "Disconnected from database " + url);
            } catch (SQLException sqle) {
                throw new UMLSQueryException(sqle);
            }
        }
    }

    /**
     *
     * @param code
     * @return
     * @throws UMLSQueryException
     */
    public String SCUIcodeToUID(String code, String sab) throws UMLSQueryException {
        if (code == null || code.equals("") || sab == null || sab.equals("")){
            throw new UMLSQueryException("The code and SAB must not be null");
        }
        String arr = null;
        try {
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select CUI from MRCONSO where SCUI = ? and SAB = ? limit 1");
            List<String> params = new ArrayList<String>();
            params.add(code);
            params.add(sab);
            ResultSet rs = executeAndLogQuery(substParams(sql.toString(),
                    params));
            while(rs.next()){
                arr = rs.getString(1);
            }
            return arr;
        } catch (SQLException sqle) {
            throw new UMLSQueryException(sqle);
        } finally {
            tearDownConn();
        }
    }

    public String CUItoCODE(String CUI)throws UMLSQueryException{
        if (CUI == null || CUI.equals("")){
            throw new UMLSQueryException("The CUI must not be null");
        }
        String code = null;
        try{
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select CODE from MRCONSO where CUI = ? limit 1");
            List<String> params = new ArrayList<String>();
            params.add(CUI);
            ResultSet rs = executeAndLogQuery(substParams(sql.toString(),
                    params));
            while(rs.next()){
                code = rs.getString(1);
            }
            return code;
        } catch (SQLException sqle) {
            throw new UMLSQueryException(sqle);
        } finally {
            tearDownConn();
        }
    }

    private String singletonOrSetClause(String uidKeyName, int setSize) {
        if (setSize > 1) {
            StringBuilder clause = new StringBuilder(uidKeyName + " in (");
            for (int i = 0; i < setSize - 1; i++) {
                clause.append("?, ");
            }
            clause.append("?)");

            return clause.toString();
        } else {
            return uidKeyName + " = ?";
        }
    }

    private PreparedStatement substParams(String sql,
                                          List<String> params) throws SQLException {
        PreparedStatement query = conn.prepareStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            query.setString(1 + i, params.get(i));
        }

        return query;
    }

    private ResultSet executeAndLogQuery(PreparedStatement query)
            throws SQLException {
//        System.out.println(query);
//        log(Level.FINE, "Executing query: " + query);
        return query.executeQuery();
    }
}
