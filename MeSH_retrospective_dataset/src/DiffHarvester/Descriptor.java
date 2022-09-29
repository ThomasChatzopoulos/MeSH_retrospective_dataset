package DiffHarvester;

import help.Helper;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *  A class representing a Descriptor in MeSH
 * @author tasosnent
 */
public class Descriptor {
    private ArrayList <String> ConceptUIs;
    private ArrayList <String> ConceptNames;
    private ArrayList <String> ConceptPreferredConceptYN;
    private ArrayList <String> TreeNumbers;
    private ArrayList <String> Categories;
    private ArrayList <String> Terms;
    private ArrayList <PreviousIndexing> PreviousIndexingList;
    private HashMap <String, String> ConceptRelations;
    private String DescriptorUI;
    private String DescriptorName;
    private String PublicMeSHNote;
    private String PreferredConceptName;
    private String PreferredConceptUI;

    // if the concepts was previously a subordinate concept
    private Descriptor PreviousConceptHost;
    private String PreviousConceptRelation;
    // if the concept was previously an SCR
    private SCR previousSCR;

    @Override
    public String toString(){
        return getDescriptorName() + "(" + getDescriptorUI() + ")";
    }

    /**
     *  Constructor for a descriptor object from a JSONObject  
     * @param o     An Object representing a JSONObject as read by the Harvester containing all the information necessary to initialize a Descriptor object
     */
    public Descriptor(Object o){
        previousSCR = null;
        JSONObject jo = (JSONObject)o;
        DescriptorUI = Helper.getString("DescriptorUI", jo);
        PublicMeSHNote = Helper.getString("PublicMeSHNote", jo);
        DescriptorName = Helper.getString("DescriptorName_String", jo);
        ConceptUIs = new ArrayList <>();
        ConceptNames = new ArrayList <>();
        TreeNumbers = new ArrayList <>();
        Categories = new ArrayList <>();
        Terms = new ArrayList <>();
        PreviousIndexingList = new ArrayList <>();
        ConceptPreferredConceptYN = new ArrayList <>();
        JSONArray ConceptUIs = Helper.getJSONArray("ConceptUI", jo);
        JSONArray ConceptNames = Helper.getJSONArray("ConceptName_String", jo);
        JSONArray prefferedConceptsYN = Helper.getJSONArray("Concept~PreferredConceptYN", jo);
        JSONArray TreeNumbers = Helper.getJSONArray("TreeNumber", jo);
        JSONArray PreviousIndexings = Helper.getJSONArray("PreviousIndexing", jo);
        JSONArray Terms = Helper.getJSONArray("Term_String", jo);
        this.ConceptUIs.addAll(ConceptUIs);
        this.ConceptNames.addAll(ConceptNames);
        this.Terms.addAll(Terms);
        if(PreviousIndexings != null){
            this.PreviousIndexingList = PreviousIndexing.readPIList(PreviousIndexings);
//            System.out.println(" > " + DescriptorUI + "-" + DescriptorName + " " + PreviousIndexingList );
        }
        if(TreeNumbers != null){
            this.TreeNumbers.addAll(TreeNumbers);
        } else {
            System.out.println("\t\tWarning: " + DescriptorUI + "-" + DescriptorName + " has no tree number!" );
        }
        this.ConceptPreferredConceptYN.addAll(prefferedConceptsYN);

        //find prefferred
        for(int i = 0; i < ConceptUIs.size(); i++){
            if(prefferedConceptsYN.get(i).toString().equals("Y")){
                PreferredConceptName = ConceptNames.get(i).toString();
                PreferredConceptUI = ConceptUIs.get(i).toString();
            }
        }

        for(String tn : this.TreeNumbers ){
            String cat = tn.substring(0,1);
            if(!Categories.contains(cat)){
                Categories.add(cat);
            }
        }

        ConceptRelations = new HashMap <> ();
        if(this.ConceptUIs.size() > 1){
            JSONArray SubordinateConceptUIs = Helper.getJSONArray("Concept2UI", jo);
            JSONArray SubordinateConceptRelations = Helper.getJSONArray("ConceptRelation~RelationName", jo);
            if(SubordinateConceptUIs.size() != SubordinateConceptRelations.size()){
                System.out.print("Error with Subordinate Concept relations for "+ DescriptorUI +" : Concept UIs are " + SubordinateConceptUIs.size() + " but relations are " + SubordinateConceptRelations.size());
            } else {
                for(int i = 0; i < SubordinateConceptUIs.size(); i++){
                    String ui = SubordinateConceptUIs.get(i).toString();
                    String rln = SubordinateConceptRelations.get(i).toString();
                    if(!ConceptRelations.keySet().contains(ui)){
                        ConceptRelations.put(ui, rln);
                    }
                }
            }
        }
    }

    @Override
    public boolean equals(Object anObject) {
        if (!(anObject instanceof Descriptor)) {
            return false;
        }
        Descriptor otherMember = (Descriptor)anObject;
        return otherMember.getDescriptorUI().equals(getDescriptorUI());
    }

    @Override
    public int hashCode() {
        return getDescriptorUI().hashCode();
    }

    /**
     * @return the ConceptUIs
     */
    public ArrayList <String> getConceptUIs() {
        return ConceptUIs;
    }

    /**
     * @param ConceptUIs the ConceptUIs to set
     */
    public void setConceptUIs(ArrayList <String> ConceptUIs) {
        this.ConceptUIs = ConceptUIs;
    }

    /**
     * @return the ConceptNames
     */
    public ArrayList <String> getConceptNames() {
        return ConceptNames;
    }

    /**
     * @param ConceptNames the ConceptNames to set
     */
    public void setConceptNames(ArrayList <String> ConceptNames) {
        this.ConceptNames = ConceptNames;
    }

    /**
     * @return the ConceptPreferredConceptYN
     */
    public ArrayList <String> getConceptPreferredConceptYN() {
        return ConceptPreferredConceptYN;
    }

    /**
     * @param ConceptPreferredConceptYN the ConceptPreferredConceptYN to set
     */
    public void setConceptPreferredConceptYN(ArrayList <String> ConceptPreferredConceptYN) {
        this.ConceptPreferredConceptYN = ConceptPreferredConceptYN;
    }

    /**
     * @return the DescriptorUI
     */
    public String getDescriptorUI() {
        return DescriptorUI;
    }

    /**
     * @param DescriptorUI the DescriptorUI to set
     */
    public void setDescriptorUI(String DescriptorUI) {
        this.DescriptorUI = DescriptorUI;
    }

    /**
     * @return the DescriptorName
     */
    public String getDescriptorName() {
        return DescriptorName;
    }

    /**
     * @param DescriptorName the DescriptorName to set
     */
    public void setDescriptorName(String DescriptorName) {
        this.DescriptorName = DescriptorName;
    }

    /**
     * @return the PreferredConceptName
     */
    public String getPreferredConceptName() {
        return PreferredConceptName;
    }

    /**
     * @param PreferredConceptName the PreferredConceptName to set
     */
    public void setPreferredConceptName(String PreferredConceptName) {
        this.PreferredConceptName = PreferredConceptName;
    }

    /**
     * @return the PreferredConceptUI
     */
    public String getPreferredConceptUI() {
        return PreferredConceptUI;
    }

    /**
     * @param PreferredConceptUI the PreferredConceptUI to set
     */
    public void setPreferredConceptUI(String PreferredConceptUI) {
        this.PreferredConceptUI = PreferredConceptUI;
    }

    /**
     * @return the TreeNumbers
     */
    public ArrayList <String> getTreeNumbers() {
        return TreeNumbers;
    }

    /**
     * @param TreeNumbers the TreeNumbers to set
     */
    public void setTreeNumbers(ArrayList <String> TreeNumbers) {
        this.TreeNumbers = TreeNumbers;
    }

    /**
     * @return the Categories
     */
    public ArrayList <String> getCategories() {
        return Categories;
    }

    /**
     * @param Categories the Categories to set
     */
    public void setCategories(ArrayList <String> Categories) {
        this.Categories = Categories;
    }

    /**
     * @return the PublicMeSHNote
     */
    public String getPublicMeSHNote() {
        return PublicMeSHNote;
    }

    /**
     * @param PublicMeSHNote the PublicMeSHNote to set
     */
    public void setPublicMeSHNote(String PublicMeSHNote) {
        this.PublicMeSHNote = PublicMeSHNote;
    }

    /**
     * @return the PreviousConceptHost
     */
    public Descriptor getPreviousConceptHost() {
        return PreviousConceptHost;
    }

    /**
     * @param PreviousConceptHost the PreviousConceptHost to set
     */
    public void setPreviousConceptHost(Descriptor PreviousConceptHost) {
        this.PreviousConceptHost = PreviousConceptHost;
    }

    /**
     * @return the PreviousConceptRelation
     */
    public String getPreviousConceptRelation() {
        return PreviousConceptRelation;
    }

    /**
     * @param PreviousConceptRelation the PreviousConceptRelation to set
     */
    public void setPreviousConceptRelation(String PreviousConceptRelation) {
        this.PreviousConceptRelation = PreviousConceptRelation;
    }

    /**
     * @return the ConceptRelations
     */
    public HashMap <String, String> getConceptRelations() {
        return ConceptRelations;
    }

    /**
     * @param ConceptRelations the ConceptRelations to set
     */
    public void setConceptRelations(HashMap <String, String> ConceptRelations) {
        this.ConceptRelations = ConceptRelations;
    }

    /**
     * @return the PreviousIndexingList
     */
    public ArrayList <PreviousIndexing> getPreviousIndexingList() {
        return PreviousIndexingList;
    }

    /**
     * @param PreviousIndexingList the PreviousIndexingList to set
     */
    public void setPreviousIndexingList(ArrayList <PreviousIndexing> PreviousIndexingList) {
        this.PreviousIndexingList = PreviousIndexingList;
    }

    /**
     * @return the Terms
     */
    public ArrayList <String> getTerms() {
        return Terms;
    }

    /**
     * @param Terms the Terms to set
     */
    public void setTerms(ArrayList <String> Terms) {
        this.Terms = Terms;
    }

    /**
     * @return the previousSCR
     */
    public SCR getPreviousSCR() {
        return previousSCR;
    }

    /**
     * @param previousSCR the previousSCR to set
     */
    public void setPreviousSCR(SCR previousSCR) {
        this.previousSCR = previousSCR;
    }

}
