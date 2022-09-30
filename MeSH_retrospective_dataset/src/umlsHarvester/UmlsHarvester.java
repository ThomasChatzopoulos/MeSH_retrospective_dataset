/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umlsHarvester;

//import edu.emory.cci.aiw.umls.*;
// import mysql-connector-java-5.1.44-bin.jar
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This script is used to get information from a local database of UMLS in MySQL
 *      To create the MySQL database of UMLS, use the MetamorphoSys system
 *      Add UMLS SN tables to use functions for semantic types, relations etc.
 * @author tasosnent
 * @author chatzopoulos
 */
public class UmlsHarvester {
    protected UMLSDatabaseConnection conn; // The connection to the database using the UMLSQuery library

    /**
     *  Initialize a UmlsHarvester object
     *      Setup a connection to UMLS DB
     * @param dbURL         The URL for the database (e.g. jdbc:mysql://localhost/umls_ad)
     * @param user          The user name for database connection (e.g. root)
     * @param pass          The password for database connection (e.g. password)
     */
    public UmlsHarvester(String dbURL,String user, String pass){
        //Create connection to UMLS MySQL database
        conn = UMLSDatabaseConnection.getConnection(dbURL, user, pass);

    }

    public static void main(String[] args) {
        UMLSDatabaseConnection connection = UMLSDatabaseConnection.getConnection("jdbc:mysql:/**** credentials to connect to server ****/", "username", "password");
        ArrayList<String> cuisList = new ArrayList<String>();
        cuisList.addAll(Arrays.asList("C0317983","C0026481","C0524582","C0878486","C0030280","C0030281","C0017352","C0595939","C0481667","C0029020","C0999577","C1001592","C1140552","C1095823","C0999570","C0995158","C0085988","C1257954","C1257954"));

        for(String cui : cuisList){
            String code = null;
            try{
                code = connection.CUItoCODE(cui);
            }catch (UMLSQueryException ex) {
                Logger.getLogger(UmlsHarvester.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Code for cui "+cui+": "+code);
        }

    }


    /**
     * Get the CUI associated to the given Source Concept code
     *      Tested for : MESH
     * @param SourceID      The source concept id (e.g. M0000842)
     * @param sab           The source (SAB) name (e.g. "MSH2017_2017_02_14")
     * @return              A ConceptUID for this concept
     */
    public String getCUIByCUID(String SourceID, String sab){
        String cui = "";
        try {
            cui = conn.SCUIcodeToUID(SourceID, sab);
        } catch (UMLSQueryException ex) {
            Logger.getLogger(UmlsHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return cui;
    }

    public String getCODEByCUI(String CUI){
        String CODE = "";
        try {
            CODE = conn.CUItoCODE(CUI);
        } catch (UMLSQueryException ex) {
            Logger.getLogger(UmlsHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return CODE;
    }
}
