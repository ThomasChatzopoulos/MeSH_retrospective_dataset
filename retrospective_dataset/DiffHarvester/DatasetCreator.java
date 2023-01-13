/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DiffHarvester;

import static DiffHarvester.MeSHDiffHarvester.getIndexPath;
import static DiffHarvester.MeSHDiffHarvester.getPathDelimiter;
import static DiffHarvester.MeSHDiffHarvester.getWorkingPath;
import static DiffHarvester.MeSHDiffHarvester.loadDescriptors;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import help.Helper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import yamlSettings.Settings;

/**
 *
 * @author chatzopoulos
 */
public class DatasetCreator {
    private static MeSHDiffHarvester MeSHDiffHarv;

    /**
     * Initialize a Harvester for different versions of MeSH
     * @param referenceVersion      The year of the reference MeSH version for provenance type calculation
     */
    DatasetCreator(MeSHDiffHarvester harvester,String referenceVersion){
        MeSHDiffHarv = harvester;
    }

    /**
     * Create retrospective datasets for Fine-grained semantic indexing
     * @param args
     * @throws CsvValidationException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws CsvValidationException, FileNotFoundException {
        System.out.println(Runtime.getRuntime().maxMemory());
        /*  Initialize  */
        Settings s = new Settings("settings.yaml");
        int referenceYear = (Integer) s.getProperty("referenceYear"); // The reference year to consider
        int oldYearInitial = (Integer)s.getProperty("oldYearInitial");
        int nowYear = (Integer)s.getProperty("nowYear");

        // Create the harvester for the given reference MeSH version
        MeSHDiffHarvester harvester = new MeSHDiffHarvester(String.valueOf(referenceYear));
        harvester.setPathDelimiter();
        DatasetCreator dsCreation = new DatasetCreator(harvester, String.valueOf(referenceYear));
        
        System.out.println("\n " + new Date().toString() + " Start working on DatasetCreation.\n");
        
        // create relevantArticles datasets from CSVs and Lucene Index
        dsCreation.createJSONDatasets(oldYearInitial, nowYear);
        
        // calculate statistics for datasets
//        dsCreation.calculateStatistics(oldYearInitial, nowYear, logFile);

        //  Dulpicate analysis and handling
//        ArrayList<String> duplicatePMIDs = new ArrayList<>();
//        duplicatePMIDs.addAll(Arrays.asList("31114674","30210784","31316749"));
//
//        dsCreation.removeDuplicatesFromDataset(" /**** path to json file ****/", duplicatePMIDs);

        System.out.println("\nΕnd of the process.");
    }

    /**
     * Create relevantArticles datasets for each year in the range of years
     * 
     * @param oldYearInitial    A start date
     * @param nowYear           An end date
     */

    public void createJSONDatasets(int oldYearInitial, int nowYear) {
        int startYear = oldYearInitial;
        int endYear = nowYear;

        System.out.println(" " + new Date().toString()+" Start running writeJSONDatasets()");

        for(int year = startYear+1; year <= endYear; year ++ ){
            writeRetroDataset(year,year-1,"train");
            writeRetroDataset(year,endYear,"test");
        }
    }

    // TODO: parameter for weak label in the settings file
    /**
     * Write a dataset for "noise" detection.
     *      That is, a dataset for learning/evaluating in which article-descriptor combinations the weak-supervision fails.
     * @param year
     * @param endYear
     */
    public void writeNoiseDataset(int year, int endYear){
        // Find all single concept leaf (SCL) descriptors introduced prior to year y
        ArrayList <Descriptor> allDescriptors = MeSHDiffHarv.getDescriptorsNow(); // All descriptors in the reference year
        HashSet <Descriptor> sclDescriptors = new HashSet <Descriptor> (); // All single concept leaf descriptors
        // First remove the ones introduced after year y - endYear -
        String XMLfileOld = MeSHDiffHarv.getInputFolder()+getPathDelimiter()+"desc"+endYear+".xml";
        ArrayList <Descriptor> descriptorsOld = loadDescriptors(XMLfileOld);
        allDescriptors.retainAll(descriptorsOld);
        // Then check for the ones that are SCL
        for(Descriptor d : allDescriptors){
            // Choose descriptors that (a) have a single concept -i.e. fine grained- AND (b) have no descendants - i.e. leafs-
            if(d.getConceptUIs().size() == 1
                    && MeSHDiffHarv.getDescendants(d).size() == 0 ){
                sclDescriptors.add(d);
            }
        }
        // Save the SCL descriptors
        System.out.println(sclDescriptors.size());// ~13.000 descriptors in total, 9158 prior to 2005
        try {
            CSVWriter sclWriter = MeSHDiffHarvester.newCSVWriter( getWorkingPath() + getPathDelimiter() + "filtering_"+endYear+"_descriptors.csv");
            sclWriter.writeNext(new String[]{"Descr. UI","Descr. Name" });
            for(Descriptor d : sclDescriptors){
                sclWriter.writeNext(new String[]{d.getDescriptorUI(), d.getDescriptorName()});
            }
            sclWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        // Get articles
        ArrayList <String> pmids = new ArrayList <> ();
        String relevantArticles = getWorkingPath() + getPathDelimiter() + "filtering_"+endYear+".json";
        if(!sclDescriptors.isEmpty()){
            HashMap<String, String> CUIsDescrHashMap = new HashMap<>();
            for(Descriptor scld : sclDescriptors){
                //                TODO: Add pagination every 1000 descriptors
//                if(CUIsDescrHashMap.size() < 100){
                    String cui = MeSHDiffHarv.getDesriptorCUI(scld);
                    CUIsDescrHashMap.put(cui,scld.getDescriptorUI());
//                }
            }
            try {
                IndexReaderWriter IRW = new IndexReaderWriter(getIndexPath(),null,null,relevantArticles,null,null,null,true,false);

                ArrayList <String> selectedDescriptors = new ArrayList <String> ();
//                selectedDescriptors.addAll(CUIsDescrHashMap.values());
                // This query retrieves all documents annoated with any SCL descriptor as golden label
                //      Explicitly covers 100% of the cases of (a) False Negative and (b) True Positive for the weak label predictions
                //      Implicitly covers a small part of the cases of (c) True Negative, through the documents selectedDescriptors for other labels
                //      It is not guaranteed that the case of (d) False Positive is sufficiently covered (it may happen for w few cases through the documents selectedDescriptors for other labels)
                String query = MeSHDiffHarv.prepareQueryForDescriptorUIs(selectedDescriptors, ""+year, ""+endYear, true);
                System.out.println(" " + new Date().toString()+" Query for filtering dataset :\n"+query);
                pmids = IRW.searchNwrite(query, CUIsDescrHashMap, true);
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else{
            System.out.println("No single-concept leaf (SCL) descriptors not found introduced prior to " + endYear);
        }
    }

    /**
     * Write a retrospective dataset for fine-grained semantic indexing in relevantArticles for the given year in relevantArticles format.
     *  Given the typeOfDataset, dataset can be "train" or "test"
     *
     * @param year          the start year
     * @param endYear       the end year
     * @param typeOfDataset the type of dataset: "train" or "test"
     */
    public void writeRetroDataset(int year, int endYear, String typeOfDataset){
        System.out.println(" " + new Date().toString()+" Writing " + typeOfDataset + " dataset from " + year +" to " + endYear );

        Date start = null;
        Date end = null;
        start = new Date();  
        String query = "";
        System.out.println(" " + new Date().toString()+" Creating " + typeOfDataset + " dataset for " + year);
        String csvFilename = getWorkingPath() + getPathDelimiter() + "UseCasesSelected_"+year+".csv";
        String JSON = getWorkingPath() + getPathDelimiter() + typeOfDataset+"_"+year+".json";

        // read csv file and grt the new fine-grained descriptors of the year        
        ArrayList <String> FGDescrOfYear = getContentFromCSVFile(csvFilename, "Descr. UI",true);

        if(!FGDescrOfYear.isEmpty()){
            ArrayList <String> previousHostsOfYear = new ArrayList <> (); // All the PHs of the selectedDescriptors usecases/descriptors (cgPH)
            ArrayList <String> phDescenants = new ArrayList <> (); // cgPH extended with all the descendants (cgPHex)
            ArrayList <String> pmids = new ArrayList <> ();
            HashMap<String, String> CUIsDescrHashMap = new HashMap<>();

            // read csv file and get unique PHs UIs
            previousHostsOfYear = getContentFromCSVFile(csvFilename, "PHs UIs",true);
            previousHostsOfYear = Helper.removeDublicatesFromArrayList(previousHostsOfYear);

            // find all the descendants of the previous host
            phDescenants = MeSHDiffHarv.getDescendantsUIs(previousHostsOfYear);

            // create descriptor info json file for the year
            createInfoFileForDataset(year, FGDescrOfYear, previousHostsOfYear, phDescenants);

            // append the previous hosts list to the previous hosts descendants
            phDescenants.addAll(previousHostsOfYear);

            // Map CUIs and FG Descr from csv file
            CUIsDescrHashMap = getCUIsHashMap(getContentFromCSVFile(csvFilename, "CUI",true),getContentFromCSVFile(csvFilename, "Descr. UI",true));
            
            // run query and create dataset
            try{
                IndexReaderWriter IRW = new IndexReaderWriter(getIndexPath(),null,null,JSON,null,null,null,true,false);
                if(typeOfDataset.equals("train")){
                    query = MeSHDiffHarv.prepareQueryForDescriptorUIs(phDescenants, MeSHDiffHarv.getDate0(), Integer.toString(endYear), false);
                    System.out.println(" " + new Date().toString()+" Query for training dataset ("+year+"):\n"+query);
                    pmids = IRW.searchNwrite(query, CUIsDescrHashMap, true);

                } else if(typeOfDataset.equals("test")){
                    query = MeSHDiffHarv.prepareQueryForDescriptorUIs(phDescenants, Integer.toString(year), Integer.toString(endYear), false);
                    System.out.println(" " + new Date().toString()+" Query for test dataset ("+year+"):\n"+query);
                    pmids = IRW.searchNwrite(query, CUIsDescrHashMap, false);
//                    pmids = IRW.searchNwrite(query, CUIsDescrHashMap, true);
                }

                ArrayList<String> duplicates = returnDublicatesFromArrayList(pmids);

                if(duplicates != null){
                    removeDuplicatesFromDataset(JSON,duplicates);
                }
            }catch (IOException e) {
                System.out.println(" " + new Date().toString() + " caught a (writing) " + e.getClass() + " with message: " + e.getMessage());
            } catch (Exception e) {
                System.out.println(" " + new Date().toString() + " caught a (writing) " + e.getClass() + " with message: " + e.getMessage());
            }
            end = new Date();
            Helper.printTime(start, end,": " + pmids.size() +" "+typeOfDataset+"ing articles for "+ year + " -" );
        } else{
            System.out.println("New fine-grained descriptors not found for this year");
        }
    }

    /**
     * Given an ArrayList including descriptors UIs, it checks witch of the descriptors are leaves
     * @param descriptorIDs String ArrayList with descriptor UIs
     * @return              Descriptors that are leaves
     */
    public ArrayList <String> keepOnlyLeaves(ArrayList <String> descriptorIDs){
        ArrayList <String> leaves = new ArrayList <>();
        for(String d : descriptorIDs){
            if(MeSHDiffHarv.getDescendants(MeSHDiffHarv.getDesciptorByID(d)).isEmpty()){
                leaves.add(d);
            }
        }
        return leaves;
    }
    
    /**
     * Map cuis from the first ArrayList with descriptor UIs from the second ArrayList
     * @param cuis          String ArrayList with cuis
     * @param FGDescriptors String ArrayList with descriptor UIs
     * @return              HashMap
     */
    public HashMap<String, String> getCUIsHashMap(ArrayList<String> cuis, ArrayList<String> FGDescriptors){
        HashMap<String, String> CUIs = new HashMap<>();
        for(int i=0; i<=cuis.size()-1;i++){
            CUIs.put(cuis.get(i),FGDescriptors.get(i));
        }
        return CUIs;
    }

    /**
     * Read csv file with the new descriptors and return ArrayList with all the content of a column for the given column
     *  
     * @param fileName          The name of the file from which we get the descriptors
     * @param column            The name of the file column that includes the UIs (e.g "Descr. UI", "PHs UIs")
     * @param keepOnlyLeaves    If true, return only content from FG Descr. that are leaves (have no Descendants)
     *
     * @return
     */
    public ArrayList<String> getContentFromCSVFile(String fileName, String column, boolean keepOnlyLeaves){
        CSVReader reader;
        ArrayList <String> descriptorsInfo = new ArrayList <> ();

        try {
            reader = new CSVReader(new FileReader(fileName));
            String[] nextLine;
            boolean headers = true;
            int DescrColumn = 0;
            int DescendsColumn = 0;

            while ((nextLine = reader.readNext()) != null) {
                // find the desired column
                if (headers) {
                    while(!nextLine[DescrColumn].equals(column) && nextLine[DescrColumn] != null){
                        DescrColumn++;
                    }
                    if(nextLine[DescrColumn]!=null){
                        System.out.println(" " + new Date().toString()+" The information for \""+column+"\" was found in the " + DescrColumn + " column.");
                    } else{
                        System.out.println(" " + new Date().toString()+" Error: There is no column \""+column+"\" in the file.");
                        break;
                    }
                    headers = false;

                    if(keepOnlyLeaves){
                        while(!nextLine[DescendsColumn].equals("#Descendant Ds") && nextLine[DescendsColumn] != null){
                            DescendsColumn++;
                        }
                        if(nextLine[DescendsColumn]!=null){
                            System.out.println(" " + new Date().toString()+"\"#PHs multirelated\" was found in the " + DescendsColumn + " column.");
                        } else{
                            System.out.println(" " + new Date().toString()+" Error: There is no \"#Descendant Ds\" column in the file for the desired information for the descriptor.");
                            break;
                        }
                    }
                    continue; // skip first line (headers)
                }
                // add the info to the list
                if(keepOnlyLeaves){
                    if(nextLine[DescendsColumn].equals("0")){
                        descriptorsInfo.add(nextLine[DescrColumn]);
                    }
                } else{
                    descriptorsInfo.add(nextLine[DescrColumn]);
                }
            }
            reader.close();
        } catch (IOException | CsvValidationException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }        

        return descriptorsInfo;
    }
    
    /**
     * Create the information file for the given year
     * @param year          The year we want to create the information file
     * @param FGDescrOfYear ArrayList containing the selected fine-grained new descriptors
     * @param previousHosts ArrayList containing the Previous hosts
     * @param phDescenants  ArrayList containing all the previous hosts descendants
     */

    public void createInfoFileForDataset(int year, ArrayList<String> FGDescrOfYear, ArrayList<String> previousHosts, ArrayList<String> phDescenants){
        System.out.println("Create descriptors JSON information file for " + year);
        BufferedWriter jsonWriter = null;
        String infoJSON = getWorkingPath() + getPathDelimiter() + "descriptorsInfo_"+year+".json";
        
        try {
            jsonWriter = new BufferedWriter(new FileWriter(infoJSON));

            jsonWriter.write("{\"Year\":"+year+",\n");

            JSONArray FGDescrOfYearJSONList = Helper.StringArrayToJSONList(Helper.ArrayListToArray(FGDescrOfYear));
            jsonWriter.write("\"New fine-grained Descriptors\":"+FGDescrOfYearJSONList+",\n");

            JSONArray phJSONList = Helper.StringArrayToJSONList(Helper.ArrayListToArray(previousHosts));
            jsonWriter.write("\"Previous hosts\":"+phJSONList+",\n");

            JSONArray phDescenantsJSONList = Helper.StringArrayToJSONList(Helper.ArrayListToArray(phDescenants));
            jsonWriter.write("\"previous hosts descendants\":"+phDescenantsJSONList+"}");
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                jsonWriter.close();
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Calculate statistics given a range of years.Assumes that the datasets of the years already exist.
     * The file of statistics includes:
        - the year
        - The total # articles
        - # articles annotated with a new descriptor of interest
        - # articles annotated with more than one new descriptor of interest
        - # articles annotated with more than one new descriptor
        - # articles per each a new descriptor of interest
     *
     * @param oldYearInitial
     * @param nowYear
     * @param logFile
     */
    public void calculateStatistics(int oldYearInitial, int nowYear, String logFile){
        System.out.println(" " + new Date().toString()+" Start running calculateStatistics()");

        int startYear = oldYearInitial;
        int endYear = nowYear;
                
        String[] trainTime = caltulateRuntime(oldYearInitial, nowYear, "train", logFile);
        String[] testTime = caltulateRuntime(oldYearInitial, nowYear, "test", logFile);
        
        for(int year = startYear+1; year <= endYear; year ++ ){
            System.out.println(" " + new Date().toString()+" Calculate statistics for " + year);
            String csvFilename = getWorkingPath() + getPathDelimiter() + "UseCasesSelected_"+year+".csv";
            
            ArrayList <String> newFGDescriptorsOfTheYear = new ArrayList <> ();
            
            // read csv file and find new Descriptor names
            newFGDescriptorsOfTheYear = Helper.removeDublicatesFromArrayList(getContentFromCSVFile(csvFilename, "Descr. UI",false));
            
            String statisticsArray[][] = initializeStatisticArray(newFGDescriptorsOfTheYear);
            
            calculateStatisticsOfTheYear(year,"train",statisticsArray,trainTime[year-oldYearInitial-1]);
            calculateStatisticsOfTheYear(year,"test",statisticsArray,testTime[year-oldYearInitial-1]);
        }
        sumUpOfStatistics(startYear,endYear);
    }
        
    /**
     * Creates and initialize a 2d String array for descriptors and # articles
     * @param newFGDescriptorsOfTheYear     List with descriptors
     * @return                              The 2d String array
     */
    public String [][] initializeStatisticArray(ArrayList <String> newFGDescriptorsOfTheYear){
        String statisticsArray[][] = new String[newFGDescriptorsOfTheYear.size()][3];
        for(int i=0; i<=statisticsArray.length-1; i++){
            statisticsArray[i][0] = newFGDescriptorsOfTheYear.get(i);   // The descriptor UI
            statisticsArray[i][1] = "0";                                // #articles/FG descr
            statisticsArray[i][2] = "0";                                // #articles/weak label
        }
        return statisticsArray;
    }

    /**
     * Read dataset and calculate the statistics and update the usecase csv file with the number of articles with weak
     * @param year                          The year of the dataset
     * @param typeOfDataset                 "train" or "test"
     * @param newFGDescriptorsOfTheYear     Array with fine-grained descriptors and # of articles
     * @param runtime                       The time it took to create dataset for the given year
    */
    public void calculateStatisticsOfTheYear(int year, String typeOfDataset, String [][] newFGDescriptorsOfTheYear, String runtime){
        String JSONFile = getWorkingPath() + getPathDelimiter() + typeOfDataset + "_"+year+".json";
        FileInputStream inputStream = null;
        Scanner sc = null;
        JSONParser parser = new JSONParser();

        int numOfArticles = 0;
        int moreThanOneNewInTheSameArticle = 0;
        int totalNumOfArticlaesWithNewFGDescr = 0;
        int numOfArticlaesWithWeakLabel = 0;
        String filesize = "0";
        String duplicates = "No";
        boolean weakLabel = false;
        ArrayList <String> moreThanOneNewInTheSameArticleList = new ArrayList<>();
        ArrayList <String> totalNumOfArticlaesWithNewFGDescrList = new ArrayList<>();
        ArrayList <String> numOfArticlaesWithWeakLabelList = new ArrayList<>();
        ArrayList <String> pmidsList = new ArrayList<>();
        JSONArray leavesList = new JSONArray();

        try {
            inputStream = new FileInputStream(JSONFile);
            sc = new Scanner(inputStream);

            while (sc.hasNextLine()) {
                String line = (sc.nextLine()).replace("},","}");
                line = line.replace("]}]}","]}");
                if(line.equals("{\"documents\":[")){
                    continue;
                }
                JSONObject documentObject = (JSONObject) parser.parse(line);

                pmidsList.add(documentObject.get("pmid").toString());

                JSONArray newFGDescriptorsFound = (JSONArray) documentObject.get("newFGDescriptors");
                if(newFGDescriptorsFound != null){
                    for(int j=0; j<=newFGDescriptorsOfTheYear.length-1;j++){ // foreach new FG Descriptors from csv file
                        int newFGDescrInTheSameArticle = 0;
                        for(Object nFG : newFGDescriptorsFound){ // foreach new FG Descriptors found in the article
                            if(nFG.equals(newFGDescriptorsOfTheYear[j][0])){
                                // how many FG Descr in the same article
                                newFGDescrInTheSameArticle++;
                                // num of articles anottated with new FG descriptors
                                totalNumOfArticlaesWithNewFGDescrList.add((documentObject.get("pmid")).toString());
                                // Which FG found
                                newFGDescriptorsOfTheYear[j][1] = Integer.toString(Integer.parseInt(newFGDescriptorsOfTheYear[j][1])+1);
                            }
                        }
                        if(newFGDescrInTheSameArticle>1){ //if more than 1 FG in the same article
                            moreThanOneNewInTheSameArticleList.add((documentObject.get("pmid")).toString());
                        }
                    }
                }
                // calculations for weak label
                JSONArray articleWeakLabelsIn;
                if(documentObject.containsKey("weakLabel")){
                    weakLabel = true;
                    articleWeakLabelsIn = (JSONArray) documentObject.get("weakLabel");
                    if(!articleWeakLabelsIn.isEmpty()){
                        numOfArticlaesWithWeakLabelList.add((documentObject.get("pmid")).toString());

                        for(int j=0; j<=newFGDescriptorsOfTheYear.length-1;j++){ //  new FG Descriptors from csv file
                            for(Object wl : articleWeakLabelsIn){ // new FG Descriptors found in the article
                                if(wl.equals(newFGDescriptorsOfTheYear[j][0])){
                                    newFGDescriptorsOfTheYear[j][2] = Integer.toString(Integer.parseInt(newFGDescriptorsOfTheYear[j][2])+1);
                                }
                            }
                        }
                    }
                }
            }

            numOfArticles = pmidsList.size();
            leavesList = Helper.StringArrayToJSONList(Helper.ArrayListToArray(checkIfLeaves(newFGDescriptorsOfTheYear)));

            // remove posible dublicates from lists
            totalNumOfArticlaesWithNewFGDescr = (Helper.removeDublicatesFromArrayList(totalNumOfArticlaesWithNewFGDescrList)).size();
            moreThanOneNewInTheSameArticle = (Helper.removeDublicatesFromArrayList(moreThanOneNewInTheSameArticleList)).size();
            numOfArticlaesWithWeakLabel = (Helper.removeDublicatesFromArrayList(numOfArticlaesWithWeakLabelList)).size();

            if(checkIfDuplicateArticles(pmidsList)){
                duplicates = "Yes";
                ArrayList<String> dupl = returnDublicatesFromArrayList(pmidsList);
                System.out.println("Duplicate articles:");
                for(String d : dupl){
                    System.out.println(d);
                }
                System.out.println("End of duplicate articles.");
            }

            filesize = Helper.getFileSizeInMB(JSONFile);

            writeStatisticFile(year, typeOfDataset,filesize, numOfArticles, totalNumOfArticlaesWithNewFGDescr, moreThanOneNewInTheSameArticle, weakLabel, newFGDescriptorsOfTheYear, numOfArticlaesWithWeakLabel, leavesList, duplicates, runtime);

            if(weakLabel){
                updateUseCasesSelectedCSVFile(year, typeOfDataset, newFGDescriptorsOfTheYear);
            }

        } catch (FileNotFoundException | ParseException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } finally{
            try {
                inputStream.close();
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Write the file with statistics
     * @param year                              The year of the dataset
     * @param typeOfDataset                     "train" or "test"
     * @param filesize                          The size of file in MegaBytes
     * @param numOfArticles                     Total # articles
     * @param totalNumOfArticlesWithNewFGDescr  # articles annotated with a new descriptor of interest
     * @param newDescriptorsOfTheYear           # articles per each a new descriptor of interest (String array)
     * @param weakLabel                         True, if weak label exist, otherwise false
     * @param moreThanOneNewInTheSameArticle    # Number of articles annotated with more than one new descriptor of interest
     * @param numOfArticlaesWithWeakLabel
     * @param leaves                            JSONArray containing the descriptors which are leaves in MeSH
     * @param duplicates                        true: if duplicate articles found, false: in any other case
     * @param runtime                           The time it took to create dataset for the given year
     */
    public void writeStatisticFile(int year, String typeOfDataset, String filesize, int numOfArticles, int totalNumOfArticlesWithNewFGDescr, int moreThanOneNewInTheSameArticle, boolean weakLabel, String [][] newDescriptorsOfTheYear, int numOfArticlaesWithWeakLabel, JSONArray leaves, String duplicates, String runtime){
        BufferedWriter jsonWriter = null;
        String statisticsJSON = getWorkingPath() + getPathDelimiter() + "statistics_"+typeOfDataset+"_"+year+".json";

        try {
            jsonWriter = new BufferedWriter(new FileWriter(statisticsJSON));

            jsonWriter.write("{\"Year\":"+ year+",\n");
            jsonWriter.write("\"Dataset\":"+"\""+typeOfDataset+"\""+",\n");
            jsonWriter.write("\"Runtime\":"+"\""+runtime+"\""+",\n");
            jsonWriter.write("\"FileSize (MB)\":"+Integer.parseInt(filesize)+",\n");
            jsonWriter.write("\"Total # articles\":"+numOfArticles+",\n");
            jsonWriter.write("\"Found duplicates\":"+"\""+duplicates+"\""+",\n");
            jsonWriter.write("\"Descriptors which are leaves\":"+leaves+",\n");
            jsonWriter.write("\"# Αrticles (unique) annotated with new fine-grained descriptors\":"+totalNumOfArticlesWithNewFGDescr+",\n");
            jsonWriter.write("\"# Αrticles (unique) annotated with more than one new fine-grained descriptor\":"+moreThanOneNewInTheSameArticle+",\n");
            if(weakLabel){
                jsonWriter.write("\"# Αrticles (unique) with weak label\":"+numOfArticlaesWithWeakLabel+",\n");
            }
            jsonWriter.append("\"Articles per new fine-grained descriptor\":[\n");
            for(int i=0; i<=newDescriptorsOfTheYear.length-1;i++){
                jsonWriter.write("{\"Descriptor UI\":"+"\""+newDescriptorsOfTheYear[i][0]+"\",");
                jsonWriter.write("\"# Articles\":"+Integer.parseInt(newDescriptorsOfTheYear[i][1]));
                if(i!=(newDescriptorsOfTheYear.length-1)){
                    jsonWriter.append("},\n");
                }
                else if(weakLabel){
                    jsonWriter.append("}],\n");
                }
                else{
                    jsonWriter.append("}]");
                }
            }

            if(weakLabel){
                jsonWriter.append("\"Articles per weak-label\":[\n");
                for(int i=0; i<=newDescriptorsOfTheYear.length-1;i++){
                    jsonWriter.write("{\"Descriptor UI\":"+"\""+newDescriptorsOfTheYear[i][0]+"\",");
                    jsonWriter.write("\"# Articles\":"+Integer.parseInt(newDescriptorsOfTheYear[i][2]));
                    if(i!=(newDescriptorsOfTheYear.length-1)){
                        jsonWriter.append("},\n");
                    }
                    else{
                        jsonWriter.append("}]");
                    }
                }
            }
            jsonWriter.write("}");
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                jsonWriter.close();
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
        
    /**
     * Read the statistics files for the given range of years 
     * and creates one file with all the statistics for every year
     * @param oldYearInitial
     * @param nowYear
     */
    public void sumUpOfStatistics(int oldYearInitial, int nowYear){
        System.out.println(" " + new Date().toString()+" Create collection of statistics file");
        int startYear = oldYearInitial;
        int endYear = nowYear;  

        String statisticsFile = getWorkingPath() + getPathDelimiter() + "statisticsSumUp.csv";
        String descriptorsFile = getWorkingPath() + getPathDelimiter() + "descriptorsSumUp.csv";
        
        try {
            FileWriter statisticsCSVWriter;
            statisticsCSVWriter = new FileWriter(statisticsFile);           
            writeTitlesInStatisticCSV(statisticsCSVWriter);

            FileWriter descriptorsCSVWriter;
            descriptorsCSVWriter = new FileWriter(descriptorsFile);
            writeTitlesInDescriptorsCSV(descriptorsCSVWriter);

            for(int year = startYear+1; year <= endYear; year ++ ){
                writeSumUpOfStatistics(statisticsCSVWriter,descriptorsCSVWriter,year,"train");
            }
            for(int year = startYear+1; year <= endYear; year ++ ){
                writeSumUpOfStatistics(statisticsCSVWriter,descriptorsCSVWriter,year,"test");
            }
            statisticsCSVWriter.flush();
            descriptorsCSVWriter.flush();
            statisticsCSVWriter.close();
            descriptorsCSVWriter.close();
        } catch (IOException ex) {      
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /** 
     * Initialize a CSV file adding the first row with the titles of the columns
     *      Columns should match with the ones in writeFullInCSV
     * @param writer    CSVWriter to write in
     */
    public static void writeTitlesInStatisticCSV(FileWriter writer){
        
        String intial =      // Initial stats   
              "Year,"
            + "Dataset type,"
            + "# Total Articles,"
            + "# Articles with new FG Descr.,"
            + "# 1+ FG Descr,"
            + "# New FG Descr,"
            + "# Leaves,"
            + "# Articles with WL,"
            + "Duplicates,"
            + "Runtime,"
            + "Filesize(MB)";

        try {
            writer.append(intial+"\n");
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void writeTitlesInDescriptorsCSV(FileWriter writer){

        String intial =      // Initial stats
            "Year,"
            + "Dataset type,"
            + "FG Descriptors,"
            + "# Articles";        
        try {
            writer.append(intial+"\n");
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Write the sum up statistic file
     * @param statisticsCSVWriter   the csv writer for statistics
     * @param descriptorsCSVWriter  the csv writer for descriptors
     * @param year      the year we want to write statistics in the sum up file
     * @param dataset   "train" or "test"
     */
    public void writeSumUpOfStatistics(FileWriter statisticsCSVWriter, FileWriter descriptorsCSVWriter, int year, String dataset){
        JSONParser parser = new JSONParser();
        String statJSON = getWorkingPath() + getPathDelimiter() + "statistics_"+dataset+"_"+year+".json";

        try {
            FileReader reader = new FileReader(statJSON);
            JSONObject jsonObject = (JSONObject) parser.parse(reader);

            String Year = jsonObject.get("Year").toString();
            String typeOfDataset = jsonObject.get("Dataset").toString();
            String runtime = jsonObject.get("Runtime").toString();
            String Filesize = jsonObject.get("FileSize (MB)").toString();
            String totalArticles = jsonObject.get("Total # articles").toString();
            String duplicates = jsonObject.get("Found duplicates").toString();
            JSONArray leavesList = (JSONArray) jsonObject.get("Descriptors which are leaves");
            String numOfLeaves = Integer.toString(leavesList.size());
            String FGDescrArticles = jsonObject.get("# Αrticles (unique) annotated with new fine-grained descriptors").toString();
            String onePlusFG = jsonObject.get("# Αrticles (unique) annotated with more than one new fine-grained descriptor").toString();
            String numOfWL = "-";
            if(jsonObject.containsKey("# Αrticles (unique) with weak label")){
                numOfWL = jsonObject.get("# Αrticles (unique) with weak label").toString();
            }
            JSONArray FGJSONArray = (JSONArray) jsonObject.get("Articles per new fine-grained descriptor");
            String numOfNewFGDescr = Integer.toString(FGJSONArray.size());

            reader.close();

            String statisticsNextLine =
                Year + ","
                + typeOfDataset + ","
                + totalArticles + ","
                + FGDescrArticles + ","
                + onePlusFG + ","
                + numOfNewFGDescr + ","
                + numOfLeaves + ","
                + numOfWL + ","
                + duplicates + ","
                + runtime + ","
                + Filesize;

            statisticsCSVWriter.append(statisticsNextLine+"\n");

            if(descriptorsCSVWriter!=null){
                Iterator i = FGJSONArray.iterator();
                while(i.hasNext()){
                    JSONObject descriptorObject = (JSONObject) i.next();
                    String descriptorUI = (String)descriptorObject.get("Descriptor UI");
                    String numOfArticles = descriptorObject.get("# Articles").toString();

                    String descriptorsNextLine =
                        Year + ","
                        + typeOfDataset + ","
                        + descriptorUI + ","
                        + numOfArticles;

                    descriptorsCSVWriter.append(descriptorsNextLine+"\n");
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | ParseException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Update the fields for weak labels in "UseCasesSelected" csv file.
     * 
     * @param year
     * @param typeOfDataset
     * @param newDescriptorsOfTheYear   Array 1st col: descr UI, 2nd col: #articles, 3rd col: #articles with weak
     */
    public void updateUseCasesSelectedCSVFile(int year, String typeOfDataset, String [][] newDescriptorsOfTheYear){
        System.out.println(" " + new Date().toString()+" Update UseCasesSelected CSV File for "+ year);

        String csvfile = getWorkingPath() + getPathDelimiter() + "UseCasesSelected_" + year + ".csv";
        String tempfile = getWorkingPath() + getPathDelimiter() + "temp.csv";

        CSVReader reader;
        CSVWriter csvWriter = null;

        try {
            String[] nextLine;
            boolean headers = true;
            int columnToUpdate = 0;
            int DescrUIColumn = 0;

            reader = new CSVReader(new FileReader(csvfile));
            csvWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter( new FileOutputStream(tempfile), "UTF-8")),
//                    CSVWriter.DEFAULT_SEPARATOR,
                    ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            writeTitlesInUpdatedUseCasesCSV(csvWriter);

            while ((nextLine = reader.readNext()) != null) {
                // find the desired column
                if (headers) {
                    if(typeOfDataset.equals("train")){
                        while(!nextLine[columnToUpdate].equals("#Train WL") && nextLine[columnToUpdate] != null){
                            columnToUpdate++;
                        }
                    }else if(typeOfDataset.equals("test")){
                        while(!nextLine[columnToUpdate].equals("#Test WL") && nextLine[columnToUpdate] != null){
                            columnToUpdate++;
                        }
                    }
                    while(!nextLine[DescrUIColumn].equals("Descr. UI") && nextLine[DescrUIColumn] != null){
                        DescrUIColumn++;
                    }
                    headers = false;
                    continue; // skip first line (headers)
                }
                String currentDescr = nextLine[DescrUIColumn];
                int descrRowInArray = getDescrRowInArray(newDescriptorsOfTheYear, currentDescr);

                if(descrRowInArray>=0){
                    nextLine[columnToUpdate] = newDescriptorsOfTheYear[descrRowInArray][2];
//                    String newLine="";
//                    for(int i=0; i<=nextLine.length-2;i++){
//                        newLine=newLine+nextLine[i]+",";
//                    }
//                    newLine=newLine+nextLine[nextLine.length-1];
//                    String newLine=String.join(",",nextLine);

                    csvWriter.writeNext(nextLine);
                    csvWriter.flush();
                }
                else{
                    System.out.println("Error: The descriptor "+currentDescr+" was not detected in internal processes.");
                }
            }
            reader.close();
            csvWriter.close();

            Helper.renameFile(csvfile, tempfile);
        } catch (IOException | CsvValidationException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public int getDescrRowInArray(String [][] newDescriptorsOfTheYear, String currentDescr){
        int row=-1;
        for(int i=0; i<=newDescriptorsOfTheYear.length-1;i++){
            if(newDescriptorsOfTheYear[i][0].equals(currentDescr)){
                row=i;
            }
        }
        return row;
    }

    /**
     * Write titles for the updated UseCasesSelected CSV File
     * @param writer    The CSV writer
     */
    public static void writeTitlesInUpdatedUseCasesCSV(CSVWriter writer){
        String intial[] =      // Initial stats
            {"Rest. Level",
            "#Train",
            "#Train WL",
            "#Train gold",
            "#Test",
            "#Test WL",
            "#Test gold",
            "Prov. Code",
            "Prov. Category",
            "Prov. Type",
            "Conc. Rel.",
            "Descr. UI",
            "Descr. Name",
            "PH count",
            "#Ct",
            "#Parent Ds",
            "Parent Ds",
            "PHs",
            "PHs UIs",
            "#PHs multirelated",
            "PHs multirelated",
            "PH relations",
            "#Descendant Ds",
            "Descendant Ds",
            "MeSH Categories",
            "CUI",
            "MeSH note",
            "Prev. Indexing"};
        try {
            writer.writeNext(intial);
            writer.flush();
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Given an array with descriptors it checks which of them are leaves in MeSH
     * @param newFGDescriptorsOfTheYear 2d String array (descriptors|numOfArticles)
     * @return leaves                   ArrayList containing the descriptors which are leaves in MeSH
     */
    public ArrayList<String> checkIfLeaves(String [][] newFGDescriptorsOfTheYear){
        ArrayList<String> leaves = new ArrayList<>();

        for(int i=0; i<=newFGDescriptorsOfTheYear.length-1;i++){
            HashSet<Descriptor> descendants = MeSHDiffHarv.getDescendants(MeSHDiffHarv.getDesciptorByID(newFGDescriptorsOfTheYear[i][0]));
            if(descendants.isEmpty()){
                leaves.add(newFGDescriptorsOfTheYear[i][0]);
            }
        }

        return leaves;
    }

    /**
     * Find the dataset creation time (hh:mm:ss) for each year, for the given kind of datasets
     * @param oldYearInitial    a start year
     * @param nowYear           an end year
     * @param logFile           the name of the log txt file in which there are creation times
     * @param typeOfDataset     "train" or "test"
     * @return                  String array with the time
     */
    public String[] caltulateRuntime(int oldYearInitial, int nowYear, String typeOfDataset, String logFile){
        System.out.println(" " + new Date().toString()+" Calculate time for building datasets");
        String nextLine;
        int searchYear = oldYearInitial+1;
        String[] timePerYear = new String[nowYear-oldYearInitial];

        try {
            File file = new File(logFile);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            while ((nextLine = reader.readLine()) != null){
                String substring = typeOfDataset+"ing articles for " + searchYear + " - time : ";
                if(nextLine.contains(substring)){
                    String[] substrings = nextLine.split(substring);
                    timePerYear[searchYear-oldYearInitial-1]=substrings[1];
                    if(searchYear==nowYear){
                        break;
                    }else{
                        searchYear++;
                    }
                }
            }
            reader.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return timePerYear;
    }

    /**
     *  ~~  **   ~~ Find and handle duplicate PMIDs in the datasets   ~~  **  ~~
     */

    /**
     * Remove duplicates from relevantArticles dataset.
     * Because the latest version of articles that have been found to be duplicates
     * can be found anywhere on the keyboard, the file is essentially being rewrite without the duplicates.
     *
     * @param JSONFile          path to file
     * @param duplicatePMIDs    ArrayList with pmids of duplicates
     */
    public void removeDuplicatesFromDataset(String JSONFile, ArrayList<String> duplicatePMIDs){
        System.out.println(" " + new Date().toString()+" Remove duplicates from dataset...");
        FileInputStream inputStream = null;
        Scanner sc = null;
        JSONParser parser = new JSONParser();

        ArrayList<ArrayList<String>> pmidsList = IntializePMIDs2DArr(duplicatePMIDs);

        // read json and keep revised dates for duplicate PMIDs
        try {
            inputStream = new FileInputStream(JSONFile);
            sc = new Scanner(inputStream);

            while (sc.hasNextLine()) {
                String line = (sc.nextLine()).replace("},","}");
                line = line.replace("]}]}","]}");
                if(line.equals("{\"documents\":[")){
                    continue;
                }
                JSONObject documentObject = (JSONObject) parser.parse(line);
                String currentPMID = documentObject.get("pmid").toString();
                for(int i=0; i<=pmidsList.size()-1;i++){
                    if((pmidsList.get(i).get(0)).equals(currentPMID)){
                        pmidsList.get(i).add(documentObject.get("Date_revised").toString());
                    }
                }
            }
        } catch (FileNotFoundException | ParseException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } finally{
            try {
                inputStream.close();
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // keep the newest revised date for each pmid
        for(int i=0; i<=pmidsList.size()-1; i++){
            String[] newestDate = pmidsList.get(i).get(1).split("-");
            for(int j=2; j<=pmidsList.get(i).size()-1; j++){
                String[] date = (pmidsList.get(i).get(j)).split("-");
                if(Integer.parseInt(newestDate[2]) <= Integer.parseInt(date[2])){
                    if(Integer.parseInt(newestDate[1]) <= Integer.parseInt(date[1])){
                        if(Integer.parseInt(newestDate[0]) <= Integer.parseInt(date[0])){
                            newestDate[0]=date[0];
                            newestDate[1]=date[1];
                            newestDate[2]=date[2];
                        }
                    }
                }
            }
            pmidsList.get(i).subList(1,pmidsList.get(i).size()).clear();
            pmidsList.get(i).addAll(Arrays.asList((newestDate[0]+"-"+newestDate[1]+"-"+newestDate[2]),"no"));
        }

        // remove duplicates - rewrite the file
        BufferedWriter jsonWriter = null;
        String temp = getWorkingPath() + getPathDelimiter() + "temp.json";

        try {
            jsonWriter = new BufferedWriter(new FileWriter(temp));
            inputStream = new FileInputStream(JSONFile);
            sc = new Scanner(inputStream);
            while (sc.hasNextLine()) {
                boolean dup = false;
                String originalLine = sc.nextLine();
                String line = originalLine.replace("},","}");
                line = line.replace("]}]}","]}");
                //write the forst line in file
                if(line.equals("{\"documents\":[")){
                    jsonWriter.append(originalLine + "\n");
                    jsonWriter.flush();
                    continue;
                }
                //read article from json and get pmid
                JSONObject documentObject = (JSONObject) parser.parse(line);
                String currentPMID = documentObject.get("pmid").toString();
                //foreach duplicate pmid
                for(int i=0; i<=pmidsList.size()-1;i++){
                    //if duplicate pmid == article pmid
                    if((pmidsList.get(i).get(0)).equals(currentPMID)){
                        dup=true;
                        System.out.println(" " + new Date().toString()+ " pmid: "+currentPMID+" detected.");
                        //if the Date_revised is the latest and is detected for the first time
                        if(pmidsList.get(i).get(1).equals(documentObject.get("Date_revised").toString()) && pmidsList.get(i).get(2).equals("no")){
                            jsonWriter.append(originalLine + "\n");
                            jsonWriter.flush();
                            pmidsList.get(i).set(2,"yes");
                            System.out.println(" " + new Date().toString()+ " pmid: "+currentPMID+" the article was written");
                            break;
                        }
                        if(pmidsList.get(i).get(2).equals("yes")){
                            System.out.println(" " + new Date().toString()+ " pmid: "+currentPMID+" was detected again (was not written)");
                            break;
                        }
                    }
                }
                //if article is not duplicate, write it
                if(!dup){
                    jsonWriter.append(originalLine + "\n");
                    jsonWriter.flush();
                }
            }
        } catch (IOException | ParseException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                jsonWriter.close();
                inputStream.close();

                Helper.renameFile(JSONFile, temp);
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Given the pmids of the articles of the dataset, it checks if there are duplicate articles.
     * @param pmidsList     ArryList with the pmids of the articles of the dataset
     * @return              true: if duplicate articles found, false: in any other case
     */
    public boolean checkIfDuplicateArticles(ArrayList <String> pmidsList){
        return ((pmidsList).size()-(Helper.removeDublicatesFromArrayList(pmidsList)).size())>0;
    }

    /**
     * Identify duplicates
     * @param ArrList   A set of PMIDs to check for duplicates
     * @return          A set of PMIDs found multiple times in ArrList
     */
    public ArrayList<String> returnDublicatesFromArrayList(ArrayList<String> ArrList){
        ArrayList<String> unique = new ArrayList<>();
        ArrayList<String> duplicates = new ArrayList<>();
        for(String s : ArrList){
            if(!unique.contains(s)){
                unique.add(s);
            }else{
                duplicates.add(s);
            }
        }
        return duplicates;
    }

    /**
    * Check in XML files from PubMed baseline if the given PMIDs exist more than once
     * @param PMIDs ArrayList with PMIDs to search
    */
    public void checkPMIDsInXMLs(ArrayList<String> PMIDs){
        System.out.println(" " + new Date().toString()+" Start running for duplicates in xml files");
        String xmlPath = "/** path to xlm (/files/Lucene) **/";
        String nextLine;
        ArrayList<ArrayList<String>> pmidsAndXMLs = IntializePMIDs2DArr(PMIDs);

        for(int i=1;i<=1015;i++){
            String zeros = new String(new char[4-(Integer.toString(i)).length()]).replace("\0", "0");
            String filename = "pubmed20n"+zeros + Integer.toString(i)+".xml";
            String fullpath = xmlPath + getPathDelimiter() + filename;
            System.out.println(" " + new Date().toString()+" Search in "+ filename);
            try {
                File file = new File(fullpath);
                BufferedReader reader = new BufferedReader(new FileReader(file));
                while ((nextLine = reader.readLine()) != null){
                    if(nextLine.contains("PMID Version")){
                        for(int j=0; j<=PMIDs.size()-1;j++){
                            String[] parts = nextLine.split("<");
                            String[] pmidParts = parts[1].split(">");
                            if(pmidParts[1].equals(PMIDs.get(j))){
                                pmidsAndXMLs.get(j).add(filename);
                            }
                        }
                    }
                }
                reader.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        createFileForDuplicates(pmidsAndXMLs);
    }

    /**
     * Given an 1D ArrayList with pmids, it returns a 2D ArrayList
     * @param PMIDs ArrayList with pmids
     * @return      2D ArrayList
     */
    public  ArrayList<ArrayList<String>> IntializePMIDs2DArr(ArrayList<String> PMIDs){
        ArrayList<ArrayList<String>> pmids = new ArrayList<>();

        for(int i=0; i<=PMIDs.size()-1;i++){
            pmids.add(new ArrayList<>(Arrays.asList(PMIDs.get(i))));
        }
        return pmids;
    }

    /**
     * Given an 2D ArrayList containing:    1st column: a PMID
     *                                      next columns: names of files that the PMID found
     * creates a CSV file with the above information.Each registration concerns
     * a unique PMID.
     *
     * @param pmidsAndXMLs  A 2D ArrayList
     */
    public void createFileForDuplicates(ArrayList<ArrayList<String>> pmidsAndXMLs){
        System.out.println(" " + new Date().toString()+" Write duplicates file");
        
        String duplicatesFile = getWorkingPath() + getPathDelimiter() + "duplicates.csv";
        
        try {
            FileWriter duplicatesCSVWriter;
            duplicatesCSVWriter = new FileWriter(duplicatesFile);
            writeTitlesInDuplicatesCSV(duplicatesCSVWriter);

            for(int i=0; i<=pmidsAndXMLs.size()-1;i++){
                String pmid = pmidsAndXMLs.get(i).get(0);
                String times = Integer.toString(pmidsAndXMLs.get(i).size()-1);
                String files="";

                for(int j=1; j<=pmidsAndXMLs.get(i).size()-1;j++){
                    if(files.equals("")){
                        files = pmidsAndXMLs.get(i).get(j);
                    }else{
                        files = files +"-"+pmidsAndXMLs.get(i).get(j);
                    }
                }
                String duplicatesNextLine =
                      pmid + ","
                    + times + ","
                    + files;

                duplicatesCSVWriter.append(duplicatesNextLine+"\n");
            }

            duplicatesCSVWriter.flush();
            duplicatesCSVWriter.close();
        } catch (IOException ex) {      
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Write titles in CSV file for duplicates
     * @param writer    The CSV writer
     */
    public static void writeTitlesInDuplicatesCSV(FileWriter writer){
        String intial =      // Initial stats
            "PMID,"
            + "# Found in XML,"
            + "Files";
        try {
            writer.append(intial+"\n");
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Using the logfile, in which, after the code for calculating the statistics 
     * has been executed, the duplicates have been printed, it returns an ArrayList
     * with the duplicate articles that found.
     *
     * @param logFile   The logfile
     * @return          ArrayList with the duplicate articles that found using logfile
     */
    public ArrayList<String> duplicatesInLogFile(String logFile){
        System.out.println(" " + new Date().toString()+" Start working on duplicates in XML files:");
        ArrayList<String> pmids = new ArrayList<>();
        String nextLine;

        try {
            File file = new File(logFile);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            boolean foundDuplicates=false;
            while ((nextLine = reader.readLine()) != null){
                if(nextLine.equals("End of duplicate articles.")){
                    foundDuplicates=false;
                }
                if(foundDuplicates){
                    pmids.addAll(Arrays.asList(nextLine));
                }
                if(nextLine.equals("Duplicate articles:")){
                    foundDuplicates=true;
                }
            }
            reader.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DatasetCreator.class.getName()).log(Level.SEVERE, null, ex);
        }

        pmids = Helper.removeDublicatesFromArrayList(pmids);
        return pmids;
    }
}
