/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//Commented for Server compile
package DiffHarvester;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
//Commented for Server compile
import help.Helper;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import mongoConnect.MongoDatasetConnector;
//add lucene-core-5.3.1.jar
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
//add lucene-analyzers-common-5.3.1.jar
import org.apache.lucene.analysis.standard.StandardAnalyzer;
//add lucene-queryparser-5.3.1.jar
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.json.simple.JSONArray;
//add json-simple-1.1.1.jar
import org.json.simple.JSONObject;
//add opencsv-5.0.jar
import com.opencsv.CSVWriter;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import yamlSettings.Settings;
import smdbHarvester.SmdbHarvester;
import umlsHarvester.UmlsHarvester;
/**
 *
 * @author tasosnent
 * @author chatzopoulos
 */
public class IndexReaderWriter {

    //Hardcoded values
    private final static boolean debugMode = true; //Enables printing of messages for normal functions
    private boolean extraFields = false; //Enables including of extra fields in testset JSON (MeSH terms in each document)
    private boolean citedByFields = false; //Enables including of citedBy fields in testset JSON (MeSH terms in each document)
    private static final int hitsMax = 1000; // "searchNwrite window" searchNwrite for hitsMax top documents each time.
    private static final int maxNumberOfCachedQueries = 250;
    private static final long maxRamBytesUsed = 500 * 1024L * 1024L; // 100MB
    private static HashMap <String, String> supportedIndexFields = new <String, String> HashMap(); //Supported index Fields and their pubmed counterpart ("Title","ArticleTitle")

    private String indexPath = null;
    private IndexReader reader = null;
    private IndexSearcher searcher = null;
    private Analyzer analyzer = null;
    private QueryParser parser = null;
    private Sort sort = null;
    private static LRUQueryCache queryCache;
    private static QueryCachingPolicy defaultCachingPolicy;

    //wrtiting variables
    private String csvPath = null;
    private BufferedWriter jsonWriter = null;
    private CSVWriter  csvWriter = null;
    private boolean firstWrite = true;
    private BufferedWriter meshWriter = null;
    private boolean firstMeshWrite = true;
    private JSONArray relations; //variable to store "pmid hasMesh meshID" relations (initialized for each search)
    private MongoDatasetConnector articleCollection; // A MongoDB collection to store the harvested article JSON objects
    private MongoDatasetConnector meshCollection; // A MongoDB collection to store the harvested relation JSON objects

    // variables to make wirting into both JSON and lucene optional
    // true by default, if no path provided, becomes false and corresponfing output is not written
    private boolean json = true;
    private boolean lucene = true;
    private boolean mongodb = true;
    private boolean csv = false;

    //indexing variables
    private IndexWriter indexWriter = null;

    // Scopus harvesting variables
    private ScopusHarvester scopusHarvester = null;

    // load settings file
    private static Settings s = new Settings("settings.yaml");

    private UmlsHarvester umls;
    private String umls_mesh_version;

    /**
     *
     * @param indexPath                 The path for the old index to read
     * @param csvFile                   [if null, skip writing in CSV] the path for the new CSV to be written
     * @param aNewIndexPath             [if null, skip writing lucene index] the path for the new index to be written
     * @param jsonFile                  [if null, skip writing JSON file] the path for the JSON file for articles to be written
     * @param meshFile                  [if null, skip writing MESH in JSON file] the path for the JSON file for MESH relations to be written
     * @param articleCollection         [if null, skip writing JSON in MongoDB] the name for the MongoDB collection for articles to be written
     * @param meshCollection            [if null, skip writing MESH in MongoDB] the name for the MongoDB collection for MESH relations to be written
     * @param extraFields               Denotes that this test sets are for testing purposes, so save extra information (MesH terms in each document)
     * @param citedByFields             Denotes that citedByFields are needed
     * @throws IOException
     */
    public IndexReaderWriter(String indexPath,String csvFile, String aNewIndexPath, String jsonFile, String meshFile, MongoDatasetConnector articleCollection,MongoDatasetConnector meshCollection, Boolean extraFields, Boolean citedByFields) throws IOException{
        this.extraFields = extraFields;
        this.citedByFields = citedByFields;
        this.setIndexPath(indexPath);
        //wrtiting variables
        if(csvFile != null){
            csvWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter( new FileOutputStream(csvFile), "UTF-8")),
//                    CSVWriter.DEFAULT_SEPARATOR,
                    ';',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            csv=true;
        }
        if(jsonFile != null){
            jsonWriter = new BufferedWriter(new FileWriter(jsonFile));
            if(meshFile != null){
                this.meshWriter = new BufferedWriter(new FileWriter(meshFile));
            } else {
                System.out.println(" " + new Date().toString() + " dataWriter > Warning: The creation of a JSON file with article-to-Descriptor relations will be ommited.");
            }
        } else {
            json = false;
            // Log printing
//            if(debugMode) {
//                System.out.println(" " + new Date().toString() + " IndexReaderWriter > write JSON file skipped");
//            }
        }
        //TO DO : add config.properties file
        //read configuration file and update static variables adequately
        // Supported index Fields and their pubmed counterpart ("Title","ArticleTitle")
        supportedIndexFields.put("Title", "ArticleTitle");
        supportedIndexFields.put("TI", "ArticleTitle");
        supportedIndexFields.put("Abstract", "AbstractText");
        supportedIndexFields.put("AB", "AbstractText");
        supportedIndexFields.put("PMID", "PMID");
        supportedIndexFields.put("UID", "PMID");

        // Lucene objects

        /* Sorting */
        // Fields used for reverse chronological sorting of results
        // This Fields are indexed with no tokenization (for each element a StringField AND a SortedDocValuesField are added)
        // Using SortField.Type.STRING is valid (as an exception) beacause years are 4-digit numbers resulting in identical String-sorting and number-sorting.
        SortField sortFieldYear = new SortField("PubDate-Year", SortField.Type.STRING, true);

        this.setSort(new Sort(sortFieldYear));

        /* Reading the index */
        this.setReader(DirectoryReader.open(FSDirectory.open(Paths.get(getIndexPath()))));
        this.setSearcher(new IndexSearcher(getReader()));
        this.setAnalyzer(new StandardAnalyzer());

        /* writing the new sub-index */
        if(aNewIndexPath != null){
            Directory newdir = MMapDirectory.open(Paths.get(aNewIndexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            indexWriter = new IndexWriter(newdir, iwc);
        } else {
            lucene = false;
            // Log printing
//            if(debugMode) {
//                System.out.println(" " + new Date().toString() + " IndexReaderWriter > write Lucene index skipped");
//            }
        }

        /* Caching */
        // these cache and policy instances can be shared across several queries and readers
        // it is fine to eg. store them into static variables
        queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
        defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
        this.getSearcher().setQueryCache(queryCache);
        this.getSearcher().setQueryCachingPolicy(defaultCachingPolicy);

        /*  PMID as default for searchNwrite */
        this.setParser( new QueryParser( "PMID", getAnalyzer()));

        // MongoDB objects
        if(articleCollection != null){
            this.articleCollection = articleCollection;
            if(meshCollection == null){
                System.out.println(" " + new Date().toString() + " dataWriter > Error!!! mongoDB collection for MeSH relations not found. Relations will be ommited.");
            } else {
                this.meshCollection = meshCollection;
            }
        } else {
            mongodb = false;
            // Log printing
//            if(debugMode) {
//                System.out.println(" " + new Date().toString() + " IndexReaderWriter > write in mongoDB skipped");
//            }
        }

        /* Harvesting Scopus Cited-by */
        this.setScopusHarvester(new ScopusHarvester());

        umls = new UmlsHarvester(s.getProperty("umls/dbname").toString(),s.getProperty("umls/dbuser").toString(),s.getProperty("umls/dbpass").toString()); // UMLS full DataBase
        umls_mesh_version = s.getProperty("umlsMeshVersion").toString();
    }
    /**
     * Test specific functions of IndexReaderWriter. Just for development stages.
     * @param args
     */
    public static void main(String[] args) {
        String ipath = "/** path to lucene index (D061686_until2019_index) **/";
        try {
            IndexReaderWriter dw = new IndexReaderWriter(ipath,"/** path to csv file (D061686_until2019.csv)**/",null,"/** path to json file (D061686_until2019.json) **/"," /** path to json file (D061686_until2019_mesh.json) **/",null,null,true,false);
            dw.searchNwrite("+AbstractText:[\\\"\\\" TO *] ", null, false);
        } catch (IOException ex) {
            Logger.getLogger(IndexReaderWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(IndexReaderWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Search in Index, in all Fields by default.
     *      Return count of results
     * @param queryString   query terms to searchNwrite
     * @return              The count of results
     * @throws Exception
     */
    public int searchCount( String queryString) throws Exception {
        ArrayList <String> pmids = new ArrayList <String> ();

        Query query = getParser().parse(queryString);
        //chache query
        query = query.rewrite(getReader());
        Query cacheQuery = queryCache.doCache(query.createWeight(getSearcher(), true), defaultCachingPolicy).getQuery();

        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(cacheQuery);

        //Search and calculate the searchNwrite time in MS
        Date startDate = new Date();
        // Collect hitsMax top Docs sorting by reverse chronological ranking (only year taken into acount so far)
        TopDocs results = getSearcher().search(constantScoreQuery, getHitsMax(), getSort(), true, false);
        Date endDate = new Date();
        // Test code Log printing
        Long timeMS = endDate.getTime()-startDate.getTime();
        int numTotalHits = results.totalHits;
        // Log printing
        if(debugMode) {
            System.out.println(" " + new Date().toString() + " Search Documents > [queryString: " + queryString + ", total matching documents: " + numTotalHits + ", time: " + timeMS + " MS]" + " > [cacheQuery.ramBytesUsed(): " + queryCache.ramBytesUsed()+"]");
        }

        return numTotalHits;
    }

    /**
     * Search in Index, in all Fields by default.Write results into JSON file and/or Lucene new index
     * @param queryString           query terms to searchNwrite
     * @param CUIsDescrHashMap      HashMap CUIs->Descriptor
     * @param addConceptOccurenceLabel
     * @return                  The list of PMIDs of the articles written as an ArrayList <String>
     * @throws Exception
     */
    public ArrayList <String> searchNwrite( String queryString , HashMap<String, String> CUIsDescrHashMap, boolean addConceptOccurenceLabel) throws Exception {
        SmdbHarvester smdb = null;
        ArrayList <String> pmids = new ArrayList <> ();
        // Initialize relations variable
        relations =  new JSONArray();
        // Log printing
        if(debugMode) {
            String writeMsg = "";
            if(json){
                writeMsg += "JSON file, ";
            }
            if(lucene){
                writeMsg += "lucene index, ";
            }
            if(mongodb){
                writeMsg += "mongo db, ";
            }
            if(csv){
                writeMsg += "csv, ";
            }
            if(writeMsg.equals("")){
                System.out.println(" " + new Date().toString() + " dataWriter > No writing " );
            } else {
                System.out.println(" " + new Date().toString() + " dataWriter > writing in " + writeMsg );
            }
        }

        // Initiate json file
        if(csv){
            csvWriter.writeNext(new String[]{"pmid", "abstract", "title"});
        }
        if(json){
            jsonWriter.append("{\"documents\":[\n");
            if(meshWriter != null ){
                meshWriter.append("{\"relations\":[\n");
            }
        }

//        Date startDateForSMDBConnection = new Date();
//        Date endDateForSMDBConnection;
        if(addConceptOccurenceLabel){
            smdb = setSMDBconnection();
//            startDateForSMDBConnection = new Date();
        }

        Query query = getParser().parse(queryString);
        //chache query
        query = query.rewrite(getReader());
        Query cacheQuery = queryCache.doCache(query.createWeight(getSearcher(), true), defaultCachingPolicy).getQuery();

        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(cacheQuery);

        //Search and calculate the searchNwrite time in MS
        Date startDate = new Date();
        // Collect hitsMax top Docs sorting by reverse chronological ranking (only year taken into acount so far)
        TopDocs results = getSearcher().search(constantScoreQuery, getHitsMax(), getSort(), true, false);
        Date endDate = new Date();

        // Test code Log printing
        Long timeMS = endDate.getTime()-startDate.getTime();

        ScoreDoc[] hits = results.scoreDocs;

        int numTotalHits = results.totalHits;
        int numOfSearches = 0;
        if(numTotalHits > getHitsMax()) {
            numOfSearches = numTotalHits/getHitsMax();
        }
        if(numTotalHits%getHitsMax() != 0) {
            numOfSearches++;
        }
        // Log printing
        if(debugMode) {
        System.out.println(" " + new Date().toString() + " Search Documents (" + numOfSearches + " search steps)> [queryString: " + queryString + ", total matching documents: " + numTotalHits + " (" + numOfSearches + " searches ), time: " + timeMS + " MS]" + " > [cacheQuery.ramBytesUsed(): " + queryCache.ramBytesUsed()+"]");
        }

        pmids.addAll(writeHits(hits, CUIsDescrHashMap, addConceptOccurenceLabel, smdb, csv, json, lucene, mongodb));
        if ( numOfSearches  > 1 ) { // we have more searches to do
            //do the required searches, for the next hitsMax top results
            ScoreDoc maxDoc = hits[hits.length-1];
            for(int i = 1; i < numOfSearches; i++ ){
                // Log printing
                if(debugMode){
                    System.out.println(" " + new Date().toString() + " Search Documents > Additional Search " + i );
                }

                startDate = new Date();
                results = getSearcher().searchAfter(maxDoc, constantScoreQuery, getHitsMax(), getSort(), true, false);
                endDate = new Date();

                timeMS = endDate.getTime()-startDate.getTime();
                hits = results.scoreDocs;
                maxDoc = hits[hits.length-1];

                pmids.addAll(writeHits(hits, CUIsDescrHashMap, addConceptOccurenceLabel, smdb, csv, json, lucene, mongodb));
            }
        }

        // finish json file writing
        if(csv){
            csvWriter.flush();
            csvWriter.close();
        }
        if(json){
            jsonWriter.append("]");
            jsonWriter.append("}");
            jsonWriter.flush();
            jsonWriter.close();

            if (meshWriter!=null){
                meshWriter.append("]");
                meshWriter.append("}");
                meshWriter.flush();
                meshWriter.close();
            }
        }
        // finish lucene wirting
        if(lucene){
            indexWriter.close();
        }
        return pmids;
    }

    /**
     * Write all hits into CSV file and/or JSON/Lucene index /mongoDB
     *      Return list of PMIDs
     * @param hits          hits found in Lucene index
     * @param csv           if true, write in csv file, else skip
     * @param json          if true, write in json file, else skip
     * @param lucene        if true, write in lucene index, else skip
     * @param mongodb       if true, write in mongoDB collection, else skip
     * @return              The list of PMIDs of the written articles as a ArrayList <String>
     * @throws IOException
     */
    private ArrayList <String> writeHits(ScoreDoc[] hits, HashMap<String, String> CUIsDescrHashMap, boolean addConceptOccurenceLabel, SmdbHarvester smdb, boolean csv, boolean json, boolean lucene, boolean mongodb) throws IOException {
        ArrayList <String> pmids = new ArrayList <> ();
        for (int i = 0; i < hits.length; i++) {
            Document doc = getSearcher().doc(hits[i].doc);
//            TO DO: Add this check so that doc with no abstracts are not included in the set!
            if (doc != null) {
                String documentAbstract = Helper.StringArrayToString(doc.getValues("AbstractText"));
                if(!documentAbstract.equals("")){
                    if(json){
                        if(firstWrite){
                            firstWrite = false;
                        } else {
                            jsonWriter.append(",\n");
                        }
                        if(this.firstMeshWrite){
                            firstMeshWrite = false;
                        } else if (meshWriter != null){
                            meshWriter.append(",\n");
                        }
                    }
                    pmids.add(writeDocument(doc, CUIsDescrHashMap, addConceptOccurenceLabel,smdb, csv, json, lucene, mongodb));
                } else {
                    // Log printing
                    System.out.println(" " + new Date().toString() + " Search Documents > Warning! [document with empty Abstract, index : " + i + " ]");
                }
            } else {
                // Log printing
                System.out.println(" " + new Date().toString() + " Search Documents > Warning! [Empty document with index : " + i + " ]");
            }
//            if(i%50==0){//call Garbage Collector per 50 articles
//                System.gc();
//            }
        }
        return pmids;
    }

    /**
     * Write this doc to JSON file and/or Lucene index /mongoDB
     * @param doc           The Lucene doc to write
     * @param json          if true, write in json file, else skip
     * @param lucene        if true, write in lucene index, else skip
     * @param mongodb           if true, write in mongoDB collection, else skip
     * @throws IOException
     */
    /**
     *
     * @param doc           The Lucene doc to write
     * @param csv           if true, write in csv file, else skip
     * @param json          if true, write in json file, else skip
     * @param lucene        if true, write in lucene index, else skip
     * @param mongodb       if true, write in mongoDB collection, else skip
     * @return              the pmid of the article as a String
     * @throws IOException
     */
    private String writeDocument(Document doc, HashMap<String, String> CUIsDescrHashMap, boolean addConceptOccurenceLabel, SmdbHarvester smdb,boolean csv, boolean json, boolean lucene, boolean mongodb ) throws IOException{
        //get fields values
        String pmid = Helper.StringArrayToString(doc.getValues("PMID")); // 1 with 2 synonyms (DeleteCitation/PMID, CommentsCorrections/PMID) but MedlineCitation/PMID is always the first encountered
//        System.out.println(new Date().toString() + " Working on pmid:" + pmid);
        String documentAbstract = Helper.StringArrayToString(doc.getValues("AbstractText")); // (?)/(*) 1 Synonym: OtherAbstract(*)/AbstractText(+) - List
        String journal = Helper.StringArrayToString(doc.getValues("Title")); // (?) No synonyms
//        String journal = doc.get("Title"); // (?) No synonyms
        String title = Helper.StringArrayToString(doc.getValues("ArticleTitle")); // 1 No synonyms

        List<String> newMeSHList = new ArrayList<>();

        String authors = "";

        String year = doc.get("PubDate-Year"); // ? with synonyms but MedlineDate
        String dateRevised = doc.get("DateRevised-Day")+"-"+doc.get("DateRevised-Month")+"-"+doc.get("DateRevised-Year");

        // If we want a JSON file to write in jsonWriter or datasetCollection
        if(csv){
            csvWriter.writeNext(new String[]{pmid, documentAbstract, title});
        }
        if(json || mongodb){
            //add field values to JSON object
            JSONObject docJSON = new JSONObject();
            //to write pmid as integer
            docJSON.put("pmid",Integer.parseInt(pmid));
            docJSON.put("title", title);
            docJSON.put("abstractText", documentAbstract);
            docJSON.put("year", year);
            docJSON.put("Date_revised", dateRevised);

            // Mesh Headings
            JSONArray descrList = Helper.StringArrayToJSONList(doc.getValues("DescriptorName"));
            if(this.extraFields){
                docJSON.put("Descriptor_names", descrList);
            }

            JSONArray descrUIList = Helper.StringArrayToJSONList(doc.getValues("DescriptorName_UI"));
            if(this.extraFields){
                docJSON.put("Descriptor_UIs", descrUIList);
            }

            // Add a field with only the new FG descr
            for (String newD : CUIsDescrHashMap.values()) { // New FG descriptors from csv
                for (Object dUI : descrUIList) {            // Article descriptors
                    if (newD.equals(dUI)) {
                        if(!newMeSHList.contains((String)dUI)) {
                            newMeSHList.add(newD);
                            break;
                        }
                    }
                }
            }

            String[] newMeSH = newMeSHList.toArray(new String[0]);
            if(this.extraFields){
                docJSON.put("newFGDescriptors", Helper.StringArrayToJSONList(newMeSH));
            }
            /**
             * Αdd the weak label if addConceptOccurenceLabel == true
             * For each doc get article CUIs given the PMID
             * and write in the weakLabel the descriptors corresponding to common CUIs
             */
            if(addConceptOccurenceLabel){
                ArrayList<String> articleCUIs = new ArrayList<>();
                ArrayList<String> weakLabelList = new ArrayList<>();
                if(CUIsDescrHashMap!=null){
                    try{
                        articleCUIs = smdb.getCUIsInPMID(Helper.StringArrayToString(doc.getValues("PMID")));
                    }catch(Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                    ArrayList<String> commonCUIsList = commonCUIs(setToArrayList(CUIsDescrHashMap.keySet()),articleCUIs);
                    if(!commonCUIsList.isEmpty()){
                        for(String cui : commonCUIsList)
                            weakLabelList.add(CUIsDescrHashMap.get(cui));
                    }

                    articleCUIs=keepOnlyCUIs(articleCUIs);

                    /*With the above code you can add all the descriptors corresponding to the article cuis*/
                    /*articleCUIs=keepOnlyCUIs(articleCUIs);
                    if(!articleCUIs.isEmpty()){
                        for(String cui : articleCUIs){
                            if(!"".equals(cui) && cui!=null){
                                String code = umls.getCODEByCUI(cui);
                                if(code!=null && !"".equals(code)){
                                    weakLabelList.add(code);
                                }
                            }
                        }
                    }*/
                }

                String[] weakLabel = weakLabelList.toArray(new String[0]);
                String[] CUIs = articleCUIs.toArray(new String[0]);
                if(this.extraFields){
                    docJSON.put("weakLabel", Helper.StringArrayToJSONList(weakLabel));
                    docJSON.put("CUIs", Helper.StringArrayToJSONList(CUIs));
                }
            }

            // TODO: Add "relations" for MeSH Publication types
            if (meshWriter!=null){
                for(Object meshID : descrList){
                    JSONObject relJSON = new JSONObject();
                    relJSON.put("s",pmid);
                    relJSON.put("p","HAS_MESH");
                    relJSON.put("o",(String)meshID);
                    relations.add(relJSON);
                    if(this.meshCollection != null){
                        meshCollection.add(relJSON.toJSONString());
                    }
                    if(this.meshWriter != null){
                        meshWriter.append(relJSON.toJSONString());
                    }
                }
            }
            // If we want to write a JSON file
            if(json){
                jsonWriter.append(docJSON.toJSONString());
                jsonWriter.flush();
            }
            // If we want to write in mongo DB collection
            if(mongodb){
                this.articleCollection.add(docJSON.toJSONString());
            }
        }

        //If we want a lucene index, write into indexwriter
        if(lucene){
            //write also to lucene index
            Document newDoc = new Document();
            Field abstractTextField = new TextField("abstractText", documentAbstract,  Field.Store.YES);
            Field journalField = new TextField("journal", journal,  Field.Store.YES);
            Field pmidField = new TextField("pmid", pmid,  Field.Store.YES);
            Field titleField = new TextField("title", title,  Field.Store.YES);
            Field yearField = new TextField("year", year,  Field.Store.YES);
            Field authorsField = new TextField("authors", authors,  Field.Store.YES);
            if(this.extraFields){
                String[] meshList = doc.getValues("DescriptorName");
                for(String meshUI : meshList){
                    Field MESHField = new TextField("Mesh_UI", meshUI,  Field.Store.YES);
                    newDoc.add(MESHField);
                }
//                Field yearField = new TextField("year", title,  Field.Store.YES);
//                newDoc.add(yearField);
            }
            newDoc.add(abstractTextField);
            newDoc.add(journalField);
            newDoc.add(pmidField);
            newDoc.add(titleField);
            newDoc.add(yearField);
            newDoc.add(authorsField);
            indexWriter.addDocument(newDoc);
        }
        return pmid;
    }

    public String[] getDescriptors(String query){
        String[] parts = query.split("\\+");
        String[] descrParts = parts[2].split("DescriptorName_UI:");
        String[] descrList = new String[descrParts.length-1];

        descrParts[descrParts.length-1]=descrParts[descrParts.length-1].replace(" )", "");
        for(int i=1; i<=descrParts.length-1; i++) {
            descrParts[i]=descrParts[i].replace("\"", "");
            descrParts[i]=descrParts[i].replace("  ", "");
            descrList[i-1]=descrParts[i];
        }

        return descrList;
    }

    /**
     * It finds the common CUIs between the new FG Descr list from csv and articleCUIs from smdb
     *
     * @param FGnewCUIs     String ArrayList with CUIs from UseCasesSelected csv file
     * @param articleCUIs   String ArrayList with article CUIs from smdb
     * @return              String ArrayList with the common CUIs
     */
    public ArrayList<String> commonCUIs(ArrayList<String> FGnewCUIs, ArrayList<String> articleCUIs){
        ArrayList<String> common = FGnewCUIs;
        articleCUIs = keepOnlyCUIs(articleCUIs);
        common.retainAll(articleCUIs);
        return common;
    }

    /**
     * Given a String ArrayList with CUIs from smdb (e.g.“C0007623:1”) 
     returns a String ArrayList with the CUIs without the parameter ":X" (e.g.“C0007623”)
     *
     * @param articleCUIs   String ArrayList with article CUIs from smdb
     * @return              String ArrayList with article CUIs without the parameter ":X"
     */
    public ArrayList<String> keepOnlyCUIs(ArrayList<String> articleCUIs){
        ArrayList<String> CUIs = new ArrayList<>();
        for(String articleCUI :articleCUIs){
            String[] parts = articleCUI.split(":");
            CUIs.add(parts[0]);
        }
        return CUIs;
    }

    /**
     * Convert a String Set to a String ArrayList
     * @param set   The String Set
     * @return      The String ArrayList
     */
    public ArrayList<String> setToArrayList(Set<String> set){
        ArrayList<String> list = new ArrayList<>();
        for(String s : set){
            list.add(s);
        }
        return list;
    }

    public SmdbHarvester setSMDBconnection() {
        SmdbHarvester smdb = new SmdbHarvester(s.getProperty("smdb/dbname").toString(),s.getProperty("smdb/dbuser").toString(),s.getProperty("smdb/dbpass").toString()); // SemMedDB full DataBase
        System.out.println(new Date().toString() + " SmdbHarvester created");
        return smdb;
    }

    /**
     * Get a diff between two dates
     * @param date1 the oldest date
     * @param date2 the newest date
     * @param timeUnit the unit in which you want the diff
     * @return the diff value, in the provided unit
     */
    public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }

    /**
     * @return the indexPath
     */
    public String getIndexPath() {
        return indexPath;
    }

    /**
     * @param aIndexPath the indexPath to set
     */
    public void setIndexPath(String aIndexPath) {
        indexPath = aIndexPath;
    }

    /**
     * @return the hitsMax
     */
    public static int getHitsMax() {
        return hitsMax;
    }

    /**
     * @return the reader
     */
    public IndexReader getReader() {
        return reader;
    }

    /**
     * @param reader the reader to set
     */
    public void setReader(IndexReader reader) {
        this.reader = reader;
    }

    /**
     * @return the searcher
     */
    public IndexSearcher getSearcher() {
        return searcher;
    }

    /**
     * @param searcher the searcher to set
     */
    public void setSearcher(IndexSearcher searcher) {
        this.searcher = searcher;
    }

    /**
     * @return the analyzer
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * @param analyzer the analyzer to set
     */
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    /**
     * @return the parser
     */
    public QueryParser getParser() {
        return parser;
    }

    /**
     * @param parser the parser to set
     */
    public void setParser(QueryParser parser) {
        this.parser = parser;
    }

    /**
     * @return the maxRamBytesUsed
     */
    public static long getMaxRamBytesUsed() {
        return maxRamBytesUsed;
    }

    /**
     * @return the sort
     */
    public Sort getSort() {
        return sort;
    }

    /**
     * @param sort the sort to set
     */
    public void setSort(Sort sort) {
        this.sort = sort;
    }

    /**
     * @return the scopusHarvester
     */
    public ScopusHarvester getScopusHarvester() {
        return scopusHarvester;
    }

    /**
     * @param scopusHarvester the scopusHarvester to set
     */
    public void setScopusHarvester(ScopusHarvester scopusHarvester) {
        this.scopusHarvester = scopusHarvester;
    }

    private static class ScopusHarvester {

        public ScopusHarvester() {
        }
    }
}
