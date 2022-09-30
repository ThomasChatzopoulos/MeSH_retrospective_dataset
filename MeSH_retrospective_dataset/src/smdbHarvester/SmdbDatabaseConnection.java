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
package smdbHarvester;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
//Add commons-lang3-3.4.jar

/**
 * A database-baked implementation of the {@link SemMedDBQueryExecutor} interface.
 * An instance is obtained by calling the static method {@link #getConnection}
 * with the parameters for accessing the database. Additionally, the caller must
 * pass in the database API type. Once an instance has been obtained, any of the
 * queries defined in the {@link SemMedDBQueryExecutor} interface may be executed.
 *
 *  Modified to use "com.mysql.jdbc.Driver" by default because "org.arp.javautil.sql.DatabaseAPI" didn't work
 *      Updated 08/2/2018
 * 
 * @author tasosnent
 * @author chatzopoulos
 * 
 */
public class SmdbDatabaseConnection {

    // import mysql-connector-java-5.1.44-bin.jar
    private Connection conn;
    private final String url;
    private final String user;
    private final String password;

    private static void log(Level level, String msg) {
        SmdbUtil.logger().log(level, msg);
    }

    private SmdbDatabaseConnection( String url, String user,
                                    String password) {
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
    public static SmdbDatabaseConnection getConnection(
            String url, String user, String password) {
        return new SmdbDatabaseConnection(url, user, password);
    }
    /**
     *  Function modified to use "com.mysql.jdbc.Driver" 
     * @throws SmdbQueryException
     */
    private void setupConn() throws SmdbQueryException {
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
            conn = DriverManager.getConnection(this.url+"?useSSL=false&user="+this.user+"&password="+this.password+"&autoReconnect=true&failOverReadOnly=false&maxReconnects=10");

            // Do something with the Connection
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    private void tearDownConn() throws SmdbQueryException {
        if (conn != null) {
            log(Level.FINE, "Attempting to disconnect from the database...");
            try {
                conn.close();
                log(Level.FINE, "Disconnected from database " + url);
            } catch (SQLException sqle) {
                throw new SmdbQueryException(sqle);
            }
        }
    }

    //    public ArrayList<String> getOccurrencePMIDsByCUI(String cui) throws SmdbQueryException{
//        ArrayList <String> pmids = new ArrayList <String> ();
//        try {
//            setupConn();
//            StringBuilder sql = new StringBuilder(
//                    "select distinct(e.PMID) from ENTITY as e where ");
//
//            List<String> params = new ArrayList<String>();
//            if (cui != null && !cui.isEmpty()) {
//                sql.append(" e.CUI = ? ");
//                params.add(cui);
//            }
//
//            log(Level.FINE, sql.toString());
//            System.out.println("\t>"+sql);
//            ResultSet r = executeAndLogQuery(substParams(sql.toString(), params));
//
//            while (r.next()) {
//                pmids.add(r.getString(1));
//            }
//        } catch (SQLException sqle) {
//            throw new SmdbQueryException(sqle);
//        } finally {
//            tearDownConn();
//        }
//        return pmids;    }
//    
    public ArrayList <String> getCUIOccurrencesInPMID(String pmid) throws SmdbQueryException{
        ArrayList <String> cuis = new ArrayList <String> ();
        try {
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select CUI, count(ENTITY_ID) from ENTITY ");

            List<String> params = new ArrayList<String>();
            if (pmid != null && !pmid.isEmpty()) {
                sql.append(" where PMID = ? GROUP BY CUI");
                params.add(pmid);
            }

            log(Level.FINE, sql.toString());

            ResultSet r = executeAndLogQuery(substParams(sql.toString(), params));

            while (r.next()) {
                cuis.add(r.getString(1)+":"+r.getString(2));
            }
        } catch (SQLException sqle) {
            throw new SmdbQueryException(sqle);
        } finally {
            tearDownConn();
        }
        return cuis;
    }

    public  int CheckForCUICountInPMIDList(String cui, ArrayList <String> pmids) throws SmdbQueryException{
        int count = 0;
        try {
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select count(distinct(PMID)) from ENTITY ");
// select count(distinct(PMID)) from ENTITY where PMID in ("20259990","20280101", "20991877") and CUI = "C0013264" limit 1;
            List<String> params = new ArrayList<String>();
            if (cui != null && !cui.isEmpty() && pmids != null && !pmids.isEmpty()) {
                sql.append("where CUI = ? ");
                params.add(cui);
                sql.append(" and ");
                sql.append(singletonOrSetClause("PMID", pmids.size()));
                params.addAll(pmids);
            } else {
                sql.append(" limit 5");
                throw new SmdbQueryException( " No CUI or PMIDList arguments available!!! ");
            }
            log(Level.FINE, sql.toString());

//            System.out.println(" \t>>"+substParams(sql.toString(), params));
            ResultSet r = executeAndLogQuery(substParams(sql.toString(), params));

            while (r.next()) {
                count = Integer.parseInt(r.getString(1));
            }
        } catch (SQLException sqle) {
            throw new SmdbQueryException(sqle);
        } finally {
            tearDownConn();
        }
        return count;
    }

    public  int CheckForCUIListCountInPMID(ArrayList<String> cuis, String pmid) throws SmdbQueryException{
        int count = 0;
        try {
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select count(distinct(PMID)) from ENTITY ");
// select count(distinct(PMID)) from ENTITY where PMID in ("20259990","20280101", "20991877") and CUI = "C0013264" limit 1;
            List<String> params = new ArrayList<String>();
            if (cuis != null && !cuis.isEmpty() && pmid != null && !pmid.isEmpty()) {
                sql.append("where");
                sql.append(singletonOrSetClause(" CUI", cuis.size()));
                params.addAll(cuis);
                sql.append(" and ");
                sql.append("PMID = ?");
                params.add(pmid);
            } else {
                sql.append(" limit 5");
                throw new SmdbQueryException( " No CUI or PMIDList arguments available!!! ");
            }
            log(Level.FINE, sql.toString());

//            System.out.println(" \t>>"+substParams(sql.toString(), params));

            ResultSet r = executeAndLogQuery(substParams(sql.toString(), params));

            while (r.next()) {
                count = Integer.parseInt(r.getString(1));
            }
        } catch (SQLException sqle) {
            throw new SmdbQueryException(sqle);
        } finally {
            tearDownConn();
        }
        return count;
    }


    public  ArrayList <String> CheckForCUIInPMIDList(String cui, ArrayList <String> pmids) throws SmdbQueryException{
        ArrayList <String> pmidsWithCUI = new ArrayList <String> ();
        try {
            setupConn();
            StringBuilder sql = new StringBuilder(
                    "select distinct(PMID) from ENTITY ");
// select count(distinct(PMID)) from ENTITY where PMID in ("20259990","20280101", "20991877") and CUI = "C0013264" limit 1;
            List<String> params = new ArrayList<String>();
            if (cui != null && !cui.isEmpty() && pmids != null && !pmids.isEmpty()) {
                sql.append("where CUI = ? ");
                params.add(cui);
                sql.append(" and ");
                sql.append(singletonOrSetClause("PMID", pmids.size()));
                params.addAll(pmids);
            } else {
                sql.append(" limit 5");
                throw new SmdbQueryException( " No CUI or PMIDList arguments available!!! ");
            }
            log(Level.FINE, sql.toString());

            System.out.println(" \t>>"+substParams(sql.toString(), params));
            ResultSet r = executeAndLogQuery(substParams(sql.toString(), params));

            while (r.next()) {
                pmidsWithCUI.add(r.getString(1));
            }
        } catch (SQLException sqle) {
            throw new SmdbQueryException(sqle);
        } finally {
            tearDownConn();
        }
        return pmidsWithCUI;
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
        log(Level.FINE, "Executing query: " + query);
        return query.executeQuery();
    }

}
