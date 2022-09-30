/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smdbHarvester;

import DiffHarvester.MeSHDiffHarvester;
import help.Helper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author tasosnent
 * @author chatzopoulos
 */
public class SmdbHarvester {
//    Select e.CUI, e.NAME, s.PMID from ENTITY as e, SENTENCE as s WHERE e.SENTENCE_ID = s.SENTENCE_ID and S.PMID = "xxxxx"
    protected SmdbDatabaseConnection conn; // The connection to the database using the UMLSQuery library

    /**
     *  Initialize a UmlsHarvester object
     *      Setup a connection to UMLS DB
     *      English language is used only
     * @param dbName        The name of the database (e.g. umls_ad)
     * @param user          The user name for database connection (e.g. root)
     * @param pass          The password for database connection (e.g. password)
     */
    public SmdbHarvester(String dbName,String user, String pass){
        //Create connection to UMLS MySQL database
        conn = SmdbDatabaseConnection.getConnection( dbName, user, pass);
    }
    public ArrayList <String> getCUIsInPMID(String pmid){
        ArrayList <String> cuis = new ArrayList<String>();
        try {
            cuis = conn.getCUIOccurrencesInPMID(pmid);
        } catch (SmdbQueryException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return cuis;
    }

    public boolean CheckForCUICountInPMIDList( String cui, ArrayList <String> pmids, int threshold){
        int limit = 100;
        int count = 0;
        int articles = pmids.size();
        int articlesChecked = 0;
        ArrayList <String> pmidsResult = new ArrayList<String>();

        try {
            if(pmids.size()<limit){
                count = conn.CheckForCUICountInPMIDList(cui, pmids);
                articlesChecked += pmids.size();
            } else {
//                System.out.println("\t\t breaking the PMID list for " + cui);
                ArrayList<ArrayList <String>> parts = new ArrayList<ArrayList <String>> ();
                while(!pmids.isEmpty()){
//                    System.out.println("\t\t"+pmids.size());
                    ArrayList <String> part =  new ArrayList <String>();
                    part.addAll(pmids.subList(0, Math.min(limit, pmids.size())));
                    parts.add(part);
                    pmids.subList(0, Math.min(limit, pmids.size())).clear();
                }
                for(ArrayList <String> part : parts){
//                    System.out.println("\t\t Run query for next " + part.size());
                    if(count < threshold){
                        count += conn.CheckForCUICountInPMIDList(cui,part);
                        articlesChecked += part.size();
                    }
                }
            }
        } catch (SmdbQueryException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("\t\t Checked " + articlesChecked + "/" + articles + " articles");
        return count < threshold;
    }


    public int CheckForCUICountInPMIDList( String cui, ArrayList <String> pmids){
        int limit = 100;
        int count = 0;
        ArrayList <String> pmidsResult = new ArrayList<String>();
        try {
            if(pmids.size()<limit){
                count = conn.CheckForCUICountInPMIDList(cui, pmids);
            } else {
//                System.out.println("\t\t breaking the PMID list for " + cui);
                ArrayList<ArrayList <String>> parts = new ArrayList<ArrayList <String>> ();
                while(!pmids.isEmpty()){
//                    System.out.println("\t\t"+pmids.size());
                    ArrayList <String> part =  new ArrayList <String>();
                    part.addAll(pmids.subList(0, Math.min(limit, pmids.size())));
                    parts.add(part);
                    pmids.subList(0, Math.min(limit, pmids.size())).clear();
                }
                for(ArrayList <String> part : parts){
//                    System.out.println("\t\t Run query for next " + part.size());
                    count += conn.CheckForCUICountInPMIDList(cui,part);
                }
            }
        } catch (SmdbQueryException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return count;
    }

    public int CheckForCUICountInPMIDList( ArrayList <String> cuis, String pmid){
        int limit = 100;
        int count = 0;
        ArrayList <String> pmidsResult = new ArrayList<String>();
        try {
            for(int i=0;i<=cuis.size()-1;i++){
                count = conn.CheckForCUIListCountInPMID(cuis, pmid);
            }
        } catch (SmdbQueryException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return count;
    }


    public ArrayList <String> CheckForCUIInPMIDList( String cui, ArrayList <String> pmids){
        int limit = 100;
        ArrayList <String> pmidsResult = new ArrayList<String>();
        try {
            if(pmids.size()<limit){
                pmidsResult = conn.CheckForCUIInPMIDList(cui,pmids );
            } else {
//                System.out.println("\t\t breaking the PMID list for " + cui);
                ArrayList<ArrayList <String>> parts = new ArrayList<ArrayList <String>> ();
                while(!pmids.isEmpty()){
//                    System.out.println("\t\t"+pmids.size());
                    ArrayList <String> part =  new ArrayList <String>();
                    part.addAll(pmids.subList(0, Math.min(limit, pmids.size())));
                    parts.add(part);
                    pmids.subList(0, Math.min(limit, pmids.size())).clear();
                }
                for(ArrayList <String> part : parts){
//                    System.out.println("\t\t Run query for next " + part.size());
                    pmidsResult.addAll(conn.CheckForCUIInPMIDList(cui,part ));
                }
            }
        } catch (SmdbQueryException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }
        return pmidsResult;
    }

    public ArrayList<String> getPMIDS(){
        String testJSON = "/** link to json file **/";
        ArrayList <String> pmidsList = new ArrayList<>();
        FileInputStream inputStream = null;
        Scanner sc = null;
        JSONParser parser = new JSONParser();
        int num = 0;

        try {
            inputStream = new FileInputStream(testJSON);
            sc = new Scanner(inputStream);

            while (sc.hasNextLine() && num <= 99) {
                String line = (sc.nextLine()).replace("},","}");
                line = line.replace("]}]}","]}");
                if(line.equals("{\"documents\":[")){
                    continue;
                }
                JSONObject documentObject = (JSONObject) parser.parse(line);
                pmidsList.add(documentObject.get("pmid").toString());
                num++;
            }
        }catch (FileNotFoundException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
        }

        return pmidsList;
    }

    public void test(){
        Date start = null;
        Date end = null;

        ArrayList<String> pmidsList = getPMIDS();
        ArrayList<String> yearCUIs= new ArrayList <String> ();
        yearCUIs.addAll(Arrays.asList("C0388204","C0019233","C0039410"));

        // First way: get all cuis
        start = new Date();
        for(int i=0;i<=pmidsList.size()-1;i++){
            ArrayList<String> cuis1 = getCUIsInPMID(pmidsList.get(i));
        }
        end = new Date();
        Helper.printTime(start, end,"First way: get all cuis -" );

        // Second way: count and get only common
        start = new Date();
        for(int i=0;i<=pmidsList.size()-1;i++){
            if(CheckForCUICountInPMIDList(yearCUIs,pmidsList.get(i))>0){
                ArrayList<String> cuis2 = getCUIsInPMID(pmidsList.get(i));
            }
        }
        end = new Date();
        Helper.printTime(start, end,"Second way: get if cuis>0 -" );
    }

//    /**
//     * This is too Memory demanding! Don't use!
//     * @param cui
//     * @return
//     */
//    public ArrayList <String> getOccurrencePMIDsByCUI(String cui){
//        ArrayList <String> cuis = new ArrayList<String>();
//        try {
//            cuis = conn.getOccurrencePMIDsByCUI(cui);
//        } catch (SmdbQueryException ex) {
//            Logger.getLogger(SmdbHarvester.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return cuis;
//    }
    public static void main(String[] args) {
        SmdbHarvester smdb = new SmdbHarvester("jdbc:mysql: /**** credentials to connect to server ****/); // SemMedDB full DataBase
//        ArrayList <String> pmids = smdb.getOccurrencePMIDsByCUI("C0000005"); 
//        ArrayList <String> cuis = smdb.getCUIsInPMID("21955872");
        ArrayList <String> pmids = new ArrayList <String> ();
        pmids.addAll(Arrays.asList( "20259990", "20280101", "20991877"));
        ArrayList <String> elements = smdb.CheckForCUIInPMIDList("C0013264",pmids);
        System.out.println(elements);
        smdb.test();
    }

}
