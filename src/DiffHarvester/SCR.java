/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DiffHarvester;

import help.Helper;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author tasosnent
 */
public class SCR {
    private ArrayList <String> ConceptUIs;
    private ArrayList <String> ConceptNames;
    private ArrayList <String> ConceptPreferredConceptYN;
    private ArrayList <String> Terms;
    private ArrayList <String> DescriptorUIs;
    private ArrayList <String> DescriptorNames;
    private ArrayList <String> QualifierUIs;
    private HashMap <String, String> ConceptRelations;
    private String SCRUI;
    private String SCRName;
    private String PreferredConceptName;
    private String PreferredConceptUI;
    private static int hypotheticalSCRIndex = 0;

    @Override
    public String toString(){
        return getSCRName() + "(" + getSCRUI() + ")" + DescriptorUIs ;
//        return getSCRName() + "(" + getSCRUI() + ")" + DescriptorUIs + "{ " + Terms + " }";
//        return SCRName + "(" + SCRUI + ") PC: " + PreferredConceptName + ", concepts: " + ConceptUIs.size();
//        return  ConceptUIs.size() + "\t" + PreferredConceptUI+ "\t" + SCRUI + "\t" + Categories + "\t" +PublicMeSHNote + "\t" +SCRName + "\t" +PreviousIndexings;
//        return  getConceptUIs().size() + ";" + getPreferredConceptUI()+ ";" + getDescriptorUI() + ";" + getCategories() + ";\"" +getPublicMeSHNote() + "\";" +getDescriptorName() + ";\"" +getPreviousIndexings()+"\"";
    }

//    /**
//     * Construct a hypothetical Supplementary Concept Record (SCR) Object for storing a list of descriptors mapped to it.
//     * @param o     Am ArrayList <String> of the mappet descriptors.
//     */
//    public SCR(ArrayList <String> d){
////        System.out.println(jo);
//        hypotheticalSCRIndex++;
//        SCRUI = "hypotheticalSCR_"+hypotheticalSCRIndex;
//        SCRName = "hypotheticalSCR_"+hypotheticalSCRIndex;
//        DescriptorUIs = d;
//    }

    /**
     * Construct a Supplementary Concept Record (SCR) Object
     * @param o     A JSONObject with the SCR information as extracted from the corresponding XML object
     */
    public SCR(Object o){
        JSONObject jo = (JSONObject)o;
//        System.out.println(jo);
        SCRUI = Helper.getString("SupplementalRecordUI", jo);
        SCRName = Helper.getString("SupplementalRecordName_String", jo);
        ConceptUIs = new ArrayList <>();
        ConceptNames = new ArrayList <>();
        DescriptorUIs = new ArrayList <>();
        DescriptorNames = new ArrayList <>();
        QualifierUIs = new ArrayList <>();
        Terms = new ArrayList <>();
        ConceptPreferredConceptYN = new ArrayList <String>();
        JSONArray ConceptUIs = Helper.getJSONArray("ConceptUI", jo);
        JSONArray ConceptNames = Helper.getJSONArray("ConceptName_String", jo);
        JSONArray DescriptorUIs = Helper.getJSONArray("HeadingMappedTo_DescriptorReferredTo_DescriptorUI", jo);
        JSONArray DescriptorNames = Helper.getJSONArray("HeadingMappedTo_DescriptorReferredTo_DescriptorName_String", jo);
        JSONArray QualifierUIs = Helper.getJSONArray("QualifierUI", jo);
        JSONArray prefferedConceptsYN = Helper.getJSONArray("Concept~PreferredConceptYN", jo);
        JSONArray TreeNumbers = Helper.getJSONArray("TreeNumber", jo);
        JSONArray PreviousIndexings = Helper.getJSONArray("PreviousIndexing", jo);
        JSONArray Terms = Helper.getJSONArray("Term_String", jo);
        this.ConceptUIs.addAll(ConceptUIs);
        this.ConceptNames.addAll(ConceptNames);
        ArrayList <String> tmp = new ArrayList <String>();
        if(DescriptorUIs != null){
            tmp.addAll(DescriptorUIs);
            for(String ui : tmp){
                this.DescriptorUIs.add(ui.replace("*", ""));
            }
        }
        tmp = new ArrayList <String>();
        if(DescriptorNames != null){
            this.DescriptorNames.addAll(DescriptorNames);
        }
        tmp = new ArrayList <String>();
        if(QualifierUIs != null){
            tmp.addAll(QualifierUIs);
            for(String ui : tmp){
                this.QualifierUIs.add(ui.replace("*", ""));
            }
        }
        this.Terms.addAll(Terms);
        this.ConceptPreferredConceptYN.addAll(prefferedConceptsYN);

        // Connect to UMLS
        //find prefferred
        for(int i = 0; i < ConceptUIs.size(); i++){
//                    System.out.println(ConceptUIs);
            if(prefferedConceptsYN.get(i).toString().equals("Y")){
                //Concert to CUI
//                    String cui = umls.getCUIByCUID(scui, "MSH2017_2017_02_14");
//                    System.out.println(scui + " > " + cui);
                PreferredConceptName = ConceptNames.get(i).toString();
                PreferredConceptUI = ConceptUIs.get(i).toString();
            }
        }


//        this.pubmedCount = SCRHarvester.getEntrezSearchCount(this);

        ConceptRelations = new HashMap <String, String> ();
        if(this.ConceptUIs.size() > 1){
            JSONArray SubordinateConceptUIs = Helper.getJSONArray("Concept2UI", jo);
            JSONArray SubordinateConceptRelations = Helper.getJSONArray("ConceptRelation~RelationName", jo);
            if(SubordinateConceptUIs.size() != SubordinateConceptRelations.size()){
                System.out.print("Error with Subordinate Concept relations for "+ SCRUI +" : Concept UIs are " + SubordinateConceptUIs.size() + " but relations are " + SubordinateConceptRelations.size());
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
        if (!(anObject instanceof SCR)) {
            return false;
        }
        SCR otherMember = (SCR)anObject;
        return otherMember.getSCRUI().equals(getSCRUI());
    }

    @Override
    public int hashCode() {
        return getSCRUI().hashCode();
    }

    /**
     * @return the PreferredConceptUI
     */
    public String getPreferredConceptUI() {
        return PreferredConceptUI;
    }

    /**
     * @return the Terms
     */
    public ArrayList <String> getTerms() {
        return Terms;
    }

    /**
     * @return the SCRUI
     */
    public String getSCRUI() {
        return SCRUI;
    }

    /**
     * @param SCRUI the SCRUI to set
     */
    public void setSCRUI(String SCRUI) {
        this.SCRUI = SCRUI;
    }

    /**
     * @return the SCRName
     */
    public String getSCRName() {
        return SCRName;
    }

    /**
     * @param SCRName the SCRName to set
     */
    public void setSCRName(String SCRName) {
        this.SCRName = SCRName;
    }

    /**
     * @return the DescriptorUIs
     */
    public ArrayList <String> getDescriptorUIs() {
        return DescriptorUIs;
    }

    /**
     * @param DescriptorUIs the DescriptorUIs to set
     */
    public void setDescriptorUIs(ArrayList <String> DescriptorUIs) {
        this.DescriptorUIs = DescriptorUIs;
    }

    /**
     * @return the ConceptUIs
     */
    public ArrayList <String> getConceptUIs() {
        return ConceptUIs;
    }

    /**
     * @return the DescriptorNames
     */
    public ArrayList <String> getDescriptorNames() {
        return DescriptorNames;
    }

}
