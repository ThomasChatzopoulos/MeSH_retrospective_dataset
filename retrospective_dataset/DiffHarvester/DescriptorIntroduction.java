/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DiffHarvester;

import com.opencsv.CSVWriter;
import help.Helper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.commons.lang3.ArrayUtils;

/**
 * A new descriptor introduction
 * @author tasosnent
 * @author chatzopoulos
 */
public class DescriptorIntroduction {
    private Descriptor d = null;
//    private HashSet <String> siblingConcepts = null;
    private HashSet<Descriptor> parents = null;
//    private HashSet<Descriptor> siblings = null;
    private HashSet<Descriptor> descendants = null;
    private int trainSize = -1;
    private int testSize = -1;
    private int trainGoldenSize = -1;
    private int testGoldenSize = -1;
    private int trainWsSize = -1;
    private int testWsSize = -1;
    private String level = "-";
    private String cui = "-";
    private String provenanceCategory = "-";
    private HashMap <String, Integer> provenanceTypeMap = null;
    private String relation = "-";
    private MeSHDiffHarvester h;
    // The previous hosts
    private ArrayList<Descriptor> previousHostsAlive; // A list with the PHs still available i the current version
    private int previousHostsTotal = 0; // The number of total PHs identified for d, even if we don't have a descriptor object for them.
    private ArrayList<String> previousHosts;
    private HashMap <String, Descriptor> descriptorNowTerms;
    /**
     * @param d     The new descriptor of this use case
     * @param h     The harvester for providing the context (reference year, access to MeSH versions etc)
     */
    public DescriptorIntroduction(Descriptor d, MeSHDiffHarvester h){
        this.d = d;
        this.h = h;
        // Parent Descriptors
        parents = h.getParents(d);
        // Descendant Descriptors
        descendants = h.getDescendants(d);
        // Previous indexings
        ArrayList<PreviousIndexing> prevIndexingList = d.getPreviousIndexingList();
        // Previous host current position
        Descriptor host = d.getPreviousConceptHost();
        this.previousHostsAlive = new ArrayList<>();
        this.previousHosts = new ArrayList<>();
        ArrayList<String> potentialHosts = new ArrayList<>(); // Only used for Cat 3 where the hosts are initially identified as Strings 
                                                            //  and the converted to actual Desc objects, to account for PHs not matched to objectss
        provenanceTypeMap = new HashMap <>();
        
        //  ***  Identify Provenance Category   ***
        
        if(host == null){// No previous host, i.e. it was not a subordinate concept.
            // Previous state as SCR
            SCR previousSCR = d.getPreviousSCR();
            if(previousSCR == null){// It was not a SCR concept. Use the Previous Indexing
                // Find New concept categories
                if(!prevIndexingList.isEmpty()){// Category 4, new PI concept
                    // get list of Previous Indexings
                    int maxYearTo = 0;
                    for(PreviousIndexing pi : prevIndexingList){
                        String piDname = pi.getDesriptorName();
                        String yearToString = pi.getYearTo();
                        // Check if the name is still a term in any descritor
                        Descriptor categoryThreeHost = h.getDesciprotByTerm(piDname);
                        // A) Consider all Previous Indexing Descriptors as previousHostsAlive
//                        potentialHosts.add(piDname);
//                        if(pseudoHost != null){
//                            previousHostsAlive.add(pseudoHost);
//                        }
                        // B) Consider the latest Previous Indexing Descriptors as previousHostsAlive
                        if(yearToString != null){
                            // Normal previous indexing with year e.g. "specific muscles/surgery (1968-2010)"
                            int yearTo = Integer.parseInt(yearToString);
                            // Keep the most recent ones
                            if(yearTo > maxYearTo){ // A more recent descriptor found that the ones considered, keep the new and ignote the old ones.
                                previousHostsAlive.removeAll(previousHostsAlive);
                                if(categoryThreeHost != null){
                                    previousHostsAlive.add(categoryThreeHost);
                                }
                                potentialHosts.removeAll(potentialHosts);
                                potentialHosts.add(piDname);
                                maxYearTo = yearTo;
                            } else if(yearTo == maxYearTo){ // A descriptor with the same end yead found, add this in the host list.
                                potentialHosts.add(piDname);
                                if(categoryThreeHost != null){
                                    previousHostsAlive.add(categoryThreeHost);
                                }
                            } // else, it is an older descriptor than the ones considered
                        } else if(yearToString == null) {
                            // Normal previous indexing without year e.g. "specific artery"
                            if(maxYearTo == 0){// We didn't find other PI with years yet
                                potentialHosts.add(piDname);
                                if(categoryThreeHost != null){
                                    previousHostsAlive.add(categoryThreeHost);
                                }
                            }
                        }
                    }
                    provenanceCategory = "3";
                } else { // Empty PHs, Category 4 for emerging concepts
                    provenanceCategory = "4";
                }
                this.previousHostsTotal = potentialHosts.size();
                this.previousHosts.addAll(potentialHosts);
                provenanceTypeMap = findProvenanceTypeMap(h, d, previousHostsAlive, this.previousHostsTotal);

            } else {// Existing SCR turns directly into descriptor
                // get list of Previous descriptors
                for(String dui : previousSCR.getDescriptorUIs()){
                    Descriptor pseudoHost = h.getDesciptorByID(dui);
                    if(pseudoHost != null){
                        previousHostsAlive.add(pseudoHost);
                    }
                }
                provenanceCategory = "2";
                this.previousHostsTotal = previousSCR.getDescriptorUIs().size();
                this.previousHosts.addAll(previousSCR.getDescriptorNames());
                provenanceTypeMap = findProvenanceTypeMap(h, d, previousHostsAlive, this.previousHostsTotal);
            }
        } else {// Existing concept turns into descriptor
            relation = d.getPreviousConceptRelation();
            if( h.descriptorExists(host)){
                previousHostsAlive.add(h.getDesciptorByID(host.getDescriptorUI()));
            }
            provenanceCategory = "1";
            this.previousHostsTotal = 1;
            this.previousHosts.add(host.getDescriptorName());
            provenanceTypeMap = findProvenanceTypeMap(h, d, previousHostsAlive, this.previousHostsTotal);
        }

    }

    /**
     * Find Provenance Types for a given use case
     * @param h                 The harvester used as a reference point defining the "current MeSH version"
     * @param d                 The new descriptor
     * @param hosts             A list of previous previousHostsAlive for the descriptor of a use case. The descriptors MUST exist in the current version! (not checked here)
     * @param potentialHosts    The initial number of potential previous previousHostsAlive checked for the descriptor (before checking for existence in current MeSH version)
     * @return                  A map with the provenance types and corresponding counts e.g. "3.0: Emersion"-> 3 etc
     */
    public static HashMap <String, Integer> findProvenanceTypeMap(MeSHDiffHarvester h, Descriptor d, ArrayList<Descriptor> hosts, int potentialHosts ){
        HashMap <String, Integer> provenanceMap = new HashMap <> ();
        // Parent Descriptors
        HashSet<Descriptor> parents = h.getParents(d);
        // Descendant Descriptors
        HashSet<Descriptor> descendants = h.getDescendants(d);
        if(potentialHosts == 0){
            // No previous indexing term available
            provenanceMap.put("0: Emersion", 1);
        } else { // some previous indexing term available
            provenanceMap.put("0: Emersion", 0);
            // previous previousHostsAlive not existing now
            int extinctHosts = potentialHosts - hosts.size();
            if(extinctHosts >= 0){
                // these previous indexing descriptors are not used any more
                provenanceMap.put("1: Succession", extinctHosts);
            } else {
                System.out.println("\t Warning : host list is bigger than potential host list! for " + d);
            }
            // Some previous previousHostsAlive still existing now
            if(!hosts.isEmpty()){
                // Check for parents
                ArrayList <Descriptor> parentHosts = new ArrayList <> ();
                parentHosts.addAll(hosts);
                parentHosts.retainAll(parents);
                provenanceMap.put("2: Subdivision", parentHosts.size());

                // Check for ancestors (exept parents)
                ArrayList <Descriptor> ancestorHosts = new ArrayList <> ();
//                System.out.println(" >>"+previousHostsAlive);
                for(Descriptor ph : hosts){
                    // The descandnants of each host
                    if( (!parents.contains(ph)) && h.getDescendants(ph).contains(d)){
                        ancestorHosts.add(ph);
                    }
                }
                provenanceMap.put("3: Submersion", ancestorHosts.size());

                // Check descendants
                ArrayList <Descriptor> descendnantsHosts = new ArrayList <> ();
                descendnantsHosts.addAll(hosts);
                descendnantsHosts.retainAll(descendants);
                provenanceMap.put("4: Overtopping", descendnantsHosts.size());

                // Check for "other previousHostsAlive", still existing but not considered in the cases of relations above
                ArrayList <Descriptor> otherHosts = new ArrayList <> ();
                otherHosts.addAll(hosts);
                otherHosts.removeAll(parentHosts);
                otherHosts.removeAll(descendnantsHosts);
//                otherHosts.removeAll(siblingHosts);
                otherHosts.removeAll(ancestorHosts);
                provenanceMap.put("5: Detachment", otherHosts.size());

            } // else no previous previousHostsAlive to check
        }
        return provenanceMap;
    }

    /**
     * Initialize a CSV file adding the first row with the titles of the columns
     *      Columns should match with the ones in writeFullInCSV
     * @param writer    CSVWriter to write in
     */
    public static void writeTitlesInCSV(CSVWriter writer, boolean full){

        String[] intial = new String[]{     // Initial stats
            "Prov. Code"
            ,"Prov. Category"
            ,"Prov. Type"
            ,"Conc. Rel."
            ,"Descr. UI"
            ,"Descr. Name"
            ,"PH count"
            ,"#Ct"
            ,"#Parent Ds"
            ,"Parent Ds"
            ,"PHs"
            ,"PHs UIs"
            ,"#PHex"
            ,"PHex UIs"
            ,"#PHs multirelated"
            ,"PHs multirelated"
            ,"PH relations"
            ,"#PHs extended"
            ,"PHs extended"
            ,"#Descendant Ds"
            ,"Descendant Ds"
            ,"MeSH Categories"
            ,"CUI"
            ,"MeSH note"
            ,"Prev. Indexing"
        };

        if(!full){
            writer.writeNext(intial);
        } else {
            String[] additional = new String[]{   //adiditional stats
                "Rest. Level"
               ,"#Train"
               ,"#Train WL"
               ,"#Train gold"
               ,"#Test"
               ,"#Test WL"
               ,"#Test gold"
               // also included in initial stats
            };

            writer.writeNext(ArrayUtils.addAll(additional,intial));
        }
    }

    /**
     * Write the current use case as a row in an initialized CSV
     * @param writer    CSVWriter to write in
     */
    public void writeInCSV(CSVWriter writer, boolean full){
        HashSet<String> previousHostsExtended = getPreviousHostsExtendedUI();
        String[] intial = new String[]{             // Initial stats          
            this.getProvenanceSerialized()          // "Prov. Code"
            ,this.getProvenanceCategroyFull()       // "Prov. Category"
            ,this.getProvenanceTypeSerialized()     // "Prov. Type"
            ,this.getRelation()                     // "Conc. Rel."
            ,getD().getDescriptorUI()               // "Descr. UI"
            ,getD().getDescriptorName()             // "Descr. Name "
            ,"" + this.getPreviousHostsTotal()      // "PH count"
            ,"" + getD().getConceptNames().size()   // "Ct size "
            ,"" + getParents().size()               // "par Ds size "
            ,getParents().toString()                // "parent Ds "
            ,String.join(getH().getSplitChar(), this.getPreviousHosts())    // "PHs"
            ,getPreviousHostsUIs()                   //"PHs UI"
            ,"" + previousHostsExtended.size()      //"#PHex"
            ,String.join(getH().getSplitChar(), previousHostsExtended )           //"PHexUIs"
            ,"" + getPHMultirelated().size()        // "#PHs multirelated"
            ,serializeArrayList(getPHMultirelated()) // "PHs multirelated"
            ,serializeArrayList(getHierarchicalRelations()) // "PH relations"
            ,""+getPHextended().size()              //"#PHs extended"
            ,String.join("~", getPHextended()) //"PHs extended"
            ,"" + getDescendants().size()           // "#Descendant Ds"
            ,getDescendants().toString()            // "Descendant Ds"
            ,serializeArrayList(getD().getCategories()) // "MeSH Categories"
            ,getCui()                               // "CUI"
            ,getD().getPublicMeSHNote()             // "MeSH note "
            ,getD().getPreviousIndexingList().toString() // "Prev Indexing"
        };

        if(!full){
            writer.writeNext(intial);
        } else {
            String[] additional = new String[]{   //adiditional stats
                this.getLevel()                         // "Rest. Level"
                ,"" + this.getTrainSize()               // "#Train"
                ,"" + this.getTrainWsSize()             // "#Train WL"
                ,"" + this.getTrainGoldenSize()         // "#Train gold"
                ,"" + this.getTestSize()                // "#Test"
                ,"" + this.getTestWsSize()              // "#Test WL"
                ,"" + this.getTestGoldenSize()          // "#Test Gold"
            };

            writer.writeNext(ArrayUtils.addAll(additional,intial));
        }
    }

    /**
     * It finds the descriptor UI for every PH
     * @return All the desc. UIs in one string,separated by a comma
     */
    public String getPreviousHostsUIs(){
        String PHUIs=""; // String with all the desc. UIs, separated by a comma
        ArrayList<String> PHs = getPreviousHosts();

        for(int i=0; i<=PHs.size()-1; i++){
            if(h.descriptorExists(PHs.get(i))){
                if(PHUIs==""){
                    PHUIs = this.descriptorNowTerms.get(PHs.get(i)).getDescriptorUI();
                }
                else{
                    PHUIs = PHUIs + "," + descriptorNowTerms.get(PHs.get(i)).getDescriptorUI();
                }
            } else { // ph doesn't exist
                System.out.println("Descriptor-PH: \"" + PHs.get(i) + "\" not found in descriptorNowTerms HashMap");
            }
        }
        return PHUIs;
    }

    /**
     * It finds the descriptor UI for every PH
     * @return All the descr. UIs in ArrayList
     */
    public ArrayList<String> getPreviousHostsUIArrList(){
        ArrayList<String> PHUIs = new ArrayList<>();
        ArrayList<String> PHs = getPreviousHosts();

        for(int i=0; i<=PHs.size()-1; i++){
            if(h.descriptorExists(PHs.get(i))){
                PHUIs.add(descriptorNowTerms.get(PHs.get(i)).getDescriptorUI());
            } else { // ph doesn't exist
                System.out.println("Descriptor-PH: \"" + PHs.get(i) + "\" not found in descriptorNowTerms HashMap");
            }
        }
        return PHUIs;
    }

    /**
     * Check if Descriptor d exists in the current version of MeSH
     * @param d the name of a descriptor
     * @return
     */
    public boolean descriptorExists(String d){
        boolean response = false;
        if( d != null && d instanceof String && this.descriptorNowTerms.keySet().contains(d)){
            response = true;
        }
        return response;
    }

    /**
     * @return the d
     */
    public Descriptor getD() {
        return d;
    }

    /**
     * @param d the d to set
     */
    public void setD(Descriptor d) {
        this.d = d;
    }

    /**
     * @return the parents
     */
    public HashSet<Descriptor> getParents() {
        return parents;
    }

    /**
     * @param parents the parents to set
     */
    public void setParents(HashSet<Descriptor> parents) {
        this.parents = parents;
    }

    /**
     * @return the Descendant
     */
    public HashSet<Descriptor> getDescenants() {
        return getDescendants();
    }

    /**
     * @param descenants the Descendant to set
     */
    public void setDescenants(HashSet<Descriptor> descenants) {
        this.setDescendants(descenants);
    }

    /**
     * @return the trainSize
     */
    public int getTrainSize() {
        return trainSize;
    }

    /**
     * @param trainSize the trainSize to set
     */
    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    /**
     * @return the testSize
     */
    public int getTestSize() {
        return testSize;
    }

    /**
     * @param testSize the testSize to set
     */
    public void setTestSize(int testSize) {
        this.testSize = testSize;
    }

    /**
     * @return the trainGoldenSize
     */
    public int getTrainGoldenSize() {
        return trainGoldenSize;
    }

    /**
     * @param trainGoldenSize the trainGoldenSize to set
     */
    public void setTrainGoldenSize(int trainGoldenSize) {
        this.trainGoldenSize = trainGoldenSize;
    }

    /**
     * @return the testGoldenSize
     */
    public int getTestGoldenSize() {
        return testGoldenSize;
    }

    /**
     * @param testGoldenSize the testGoldenSize to set
     */
    public void setTestGoldenSize(int testGoldenSize) {
        this.testGoldenSize = testGoldenSize;
    }

    /**
     * @return the trainWsSize
     */
    public int getTrainWsSize() {
        return trainWsSize;
    }

    /**
     * @param trainWsSize the trainWsSize to set
     */
    public void setTrainWsSize(int trainWsSize) {
        this.trainWsSize = trainWsSize;
    }

    /**
     * @return the testWsSize
     */
    public int getTestWsSize() {
        return testWsSize;
    }

    /**
     * @param testWsSize the testWsSize to set
     */
    public void setTestWsSize(int testWsSize) {
        this.testWsSize = testWsSize;
    }

    /**
     * @return the level
     */
    public String getLevel() {
        return level;
    }

    /**
     * @param level the level to set
     */
    public void setLevel(String level) {
        this.level = level;
    }

    /**
     * @return the cui
     */
    public String getCui() {
        return cui;
    }

    /**
     * @param cui the cui to set
     */
    public void setCui(String cui) {
        this.cui = cui;
    }

    /**
     * @return the descendants
     */
    public HashSet<Descriptor> getDescendants() {
        return descendants;
    }

    /**
     * @param descendants the descendants to set
     */
    public void setDescendants(HashSet<Descriptor> descendants) {
        this.descendants = descendants;
    }

    /**
     * @return the relation
     */
    public String getRelation() {
        return relation;
    }

    /**
     * @param relation the relation to set
     */
    public void setRelation(String relation) {
        this.relation = relation;
    }

    /**
     *
     * @return
     */
    public String getProvenanceSerialized(){
        ArrayList <String> pts = new ArrayList <> ();
        for(String pt : this.getProvenanceTypeMap().keySet()){
            if(getProvenanceTypeMap().get(pt) > 0){
                pts.add(this.getProvenanceCategory() + "." + pt);
            }
        }
        return serializeArrayList(pts);
    }

    /**
     * @return String ArryList with UIs of the descendants of the previous host, containing the previous host
     */
    public ArrayList<String> getPHextended(){
        ArrayList<String> PHUIs = Helper.removeDublicatesFromArrayList(Helper.StringToArrayList(getPreviousHostsUIs()));
        if(!PHUIs.isEmpty()){
            ArrayList<String> descenants = h.getDescendantsUIs(PHUIs);
            PHUIs.addAll(descenants);
        }
        return PHUIs;
    }

    /**
     *
     * @return
     */
    public String getProvenanceTypeSerialized(){
        ArrayList <String> pts = new ArrayList <> ();
        for(String pt : this.getProvenanceTypeMap().keySet()){
            if(getProvenanceTypeMap().get(pt) > 0){
                pts.add(pt);
            }
        }
        return serializeArrayList(pts);
    }

    /**
     *
     * @param pts
     * @return
     */
    public String serializeArrayList(ArrayList <String> pts){
         return String.join(getH().getSplitChar(), pts);
    }
    
    /**
     * @return the provenanceCategory
     */
    public String getProvenanceCategory() {
        return provenanceCategory;
    }
    /**
     * @return the provenanceCategory
     */
    public String getProvenanceCategroyFull() {
        String pl = getProvenanceCategory();
        if(getProvenanceCategory().equals("1")){
            pl += ": Old concept";
        } else if (getProvenanceCategory().equals("2")){
            pl += ": Old SRC";
        } else if (getProvenanceCategory().equals("3")) {
            pl += ": New PI concept";
        } else {
            pl += ": New Emerging concept";
        }
        return pl;
    }

    /**
     * @param provenanceCategory the provenanceCategory to set
     */
    public void setProvenanceCategory(String provenanceCategory) {
        this.provenanceCategory = provenanceCategory;
    }

    /**
     * Get the parents with more than one hierarchical relations
     * @return
     */
    public ArrayList <String> getPHMultirelated(){
        ArrayList <String> ps = new ArrayList <> ();
        for(Descriptor p : this.getPreviousHostsAlive()){
            if( getH().getHierarchicalRelations(this.getD(), p).size()>1){
                ps.add(p.getDescriptorUI());
            }
        }
        return ps;
    }

    /**
     * Get the relations with parents with more than one hierarchical relations
     *  For each parent get all relations concatenated with a dot "."
     * @return
     */
    public ArrayList <String> getHierarchicalRelations(){
        ArrayList <String> ps = new ArrayList <> ();
        for(Descriptor p : this.getPreviousHostsAlive()){
            ps.add(String.join(".", getH().getHierarchicalRelations(this.getD(), p)));
        }
        int deadPHs = this.getPreviousHostsTotal() - this.getPreviousHostsAlive().size();
        for(int i = deadPHs; deadPHs > 0; deadPHs--){
            ps.add("und");
        }

        return ps;
    }

    /**
     * @return the provenanceTypeMap
     */
    public HashMap <String, Integer> getProvenanceTypeMap() {
        return provenanceTypeMap;
    }

    /**
     * @param provenanceTypeMap the provenanceTypeMap to set
     */
    public void setProvenanceTypeMap(HashMap <String, Integer> provenanceTypeMap) {
        this.provenanceTypeMap = provenanceTypeMap;
    }

    /**
     * @return the h
     */
    public MeSHDiffHarvester getH() {
        return h;
    }

    /**
     * @param h the h to set
     */
    public void setH(MeSHDiffHarvester h) {
        this.h = h;
    }

    /**
     * @return the previousHostsAlive
     */
    public ArrayList<Descriptor> getPreviousHostsAlive() {
        return previousHostsAlive;
    }

    /**
     * @param previousHostsAlive the previousHostsAlive to set
     */
    public void setPreviousHostsAlive(ArrayList<Descriptor> previousHostsAlive) {
        this.previousHostsAlive = previousHostsAlive;
    }

    /**
     * @return the previousHostsTotal
     */
    public int getPreviousHostsTotal() {
        return previousHostsTotal;
    }

    /**
     * @param previousHostsTotal the previousHostsTotal to set
     */
    public void setPreviousHostsTotal(int previousHostsTotal) {
        this.previousHostsTotal = previousHostsTotal;
    }

    /**
     * @return the previousHosts
     */
    public ArrayList<String> getPreviousHosts() {
        return previousHosts;
    }

    /**
     * @param previousHosts the previousHosts to set
     */
    public void setPreviousHosts(ArrayList<String> previousHosts) {
        this.previousHosts = previousHosts;
    }
    
    /**
     * @param descriptorNowTerms HashMap: DescriptorName -> Descriptor object
     */
    public void setDescriptorNowTerms(HashMap <String, Descriptor> descriptorNowTerms) {
        this.descriptorNowTerms = descriptorNowTerms;
    }

    /**
     * Get a list with the UIs of the "Extended Previous Host set" PHexUIs.
     *      That is the PHs set, extended with all the descendants of each PHs
     * @return A list of Strings, representing the UIs of the descriptors in the PHexUIs set.
     */
    private HashSet<String> getPreviousHostsExtendedUI() {

        HashSet<String> PHexUIs = new HashSet<>();

        ArrayList<String> PHs = getPreviousHosts();

        for(String PHname : PHs){
            if(this.descriptorExists(PHname)) {
                String UI = descriptorNowTerms.get(PHname).getDescriptorUI();
                // Add PH UIs
                PHexUIs.add(UI);
            } else { // ph doesn't exist
                System.out.println("Descriptor-PH: \"" + PHname + "\" not found in descriptorNowTerms HashMap");
            }
        }

        // Add PH descendant UIs
        ArrayList<String> PHexUIsList = new ArrayList<>();
        PHexUIsList.addAll(PHexUIs);
        PHexUIs.addAll(h.getDescendantsUIs(PHexUIsList));

        return PHexUIs;
    }
}
