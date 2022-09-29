/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package DiffHarvester;

import java.util.ArrayList;
import java.util.Date;
import java.util.Stack;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author tasosnent
 */
public class MeSHHandler extends DefaultHandler {

    private final JsonArrayProcessor processor;

    //Harvesting variables
    private Stack<String> elements = new Stack <String> (); // A LIFO to keep the current Element
    private Attributes attributes = null; // The attributes of the current element
    private JSONObject entity = null; // The object representing the entity we are parsing
    private StringBuffer tmpStr = new StringBuffer(); // Used to handle the fact tha Charaters() is no nececarily called once for each Element
    private StringBuffer externalIdentifierResource = new StringBuffer(); // The selecetd resources to harvest identifiers for them

    //Entities to harvest 
    // * For conflict entities use a "Parent_" prefix, except for the basic entity (e.g. Entry for uniprot)
    private ArrayList <String> storedAsStrArrays = new ArrayList <String> (); // The text content of those elements will be stored as items of a JSONArray field of The JSONObject
    private ArrayList <String> storedAsStr = new ArrayList <String> (); // The text content of those elements will be stored in a String field of The JSONObject


    // handling Entities with the same name : conflicts
    private ArrayList <String> conflictFields = new ArrayList <String> (); // Name of element that appear as sub-elements of more than one type of elements
    private String basicElement = ""; // This is the basic element in XML file (e.g. Entry for Uniprot) Hence, even for conflicts, no prefix will be used for childrens of this entity    
    private String FieldPrefixSeparator = "_" ; // This is the basic element in XML file (e.g. Entry for Uniprot) Hence, even for conflicts, no prefix will be used for childrens of this entity    

    MeSHHandler(JsonArrayProcessor processor) {
        this.processor = processor;

//      These are all The Elements that occur as subelements of more than one XML element (i.e. conflicts)
//      For those elements, the field name will have a prefix "Parent_FieldName", e.g. "kinetics_text" instead of just "text"
        this.conflictFields.add("String");
        this.conflictFields.add("DescriptorUI");
        this.conflictFields.add("DescriptorName");
//        this.conflictFields.add("average-mass");
//        this.conflictFields.add("cas-number");
//        this.conflictFields.add("category");
//        this.conflictFields.add("citation");
//        this.conflictFields.add("country");
//        this.conflictFields.add("description");



//      entity is the basic entity. Hence, for entity, no prefix will be used (i.e. no prefix, means Entry for a conflict)
        this.basicElement = "DescriptorRecord"; // Actually, this is the "entity" element
        this.FieldPrefixSeparator = "_"; // FieldPRefixes separater by "_" because "-" is already used in field names

        // *** Generalized Harvesting ***

//      Those will be String fields in the harvested objects  
        this.storedAsStr.add("DescriptorUI");
        this.storedAsStr.add("DescriptorName_String");
        this.storedAsStr.add("PublicMeSHNote");
//        this.storedAsStr.add("ConceptName_String");        
//        this.storedAsStr.add("Concept~PreferredConceptYN");    

//      Those will be Arrays of Strings in the harvested objects  
        this.storedAsStrArrays.add("ConceptUI");
        this.storedAsStrArrays.add("ConceptName_String");
        this.storedAsStrArrays.add("Concept~PreferredConceptYN"); // The Interactants from IntAct
        this.storedAsStrArrays.add("ConceptRelation~RelationName"); // The Interactants from IntAct
        this.storedAsStrArrays.add("Concept2UI"); // This is the related ConceptUI, the Concept1UI is of the preferred concept
        this.storedAsStrArrays.add("TreeNumber"); // The Interactants from IntAct
        this.storedAsStrArrays.add("PreviousIndexing"); // The Interactants from IntAct
        this.storedAsStrArrays.add("Term_String"); // The Interactants from IntAct

    }

    /**
     * Sets up entity-level variables
     *
     * @param uri
     * @param localName
     * @param qName
     * @param attributes
     * @throws SAXException
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
//        System.out.println("     " + qName + " { " );         
        startFieldName(qName);
        //initiallize attributes variable
        this.attributes = attributes;
        if (this.getFieldName().equals(this.basicElement)) {//just entred a new entity Entity
//            System.out.println("New Entry Object created" );
            //initialize Entry Object and other harvesting variables
            setObj(new JSONObject());
        } else { // just entred a new Entity, other than entity
            if(!this.insideField(this.basicElement)){//it's not a entity sub-Entity
                if (this.getFieldName().equals("DescriptorRecordSet")) {//The beginig of the process
                    System.out.println("\t *** New DescriptorRecordSet Hravesting Start ***" );
                } else {
                    //XML formar error
                    //Excecution shouldn't reach here - not a vaid XML element of the ones listed below
                    System.out.println("XML format error :\n\t" + qName + " not a valid XML Element start. ");
                }
            } //it's a entity sub-Entity
        }
        //Add attribute fields : Add Entity Name as a prefix for Attribute Fields name
        if(attributes.getLength() > 0){
            String attName = null;
            String attValue = null;
            String attFieldName = null;
            for(int i =0 ; i < attributes.getLength() ; i++){
                attName = attributes.getQName(i);
                attValue = attributes.getValue(i);
                if(attName != null & attValue != null){
                    attFieldName = this.getFieldName() + "~" + attName;

                    // Get string values for Elements to be stored as Strings
                    // E.g. for Drugbank, read an indication text
                    if(this.isStoredAsStr(attFieldName)){ // This is a field to be stored as String
                        this.addText(attFieldName, attValue);
                    } else // Not to be stored as String
                        // Get string values for Elements to be stored as Array of Strings
                        // E.g. for Uniprot, read an interactant id or an accession number
                        if(this.isStoredAsStrArray(attFieldName)){ // This is a field to be stored as String item in a JSONArray
                            this.addStringToJSONArray(attFieldName,attValue);// Strore the String in the String list of the Entry Object
                        }
                }
            }
        }
    }

    /**
     * Handles Text content of XML elements
     *
     * @param ch
     * @param start
     * @param length
     * @throws SAXException
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
//        System.out.println(" \t characters for : " + getFieldName());
        String text = new String(ch, start, length);
//        System.out.println(" \t\t text  : " + text);
        if(this.insideField(this.basicElement) ){//Inside a entity element        

            // *** Generalized Harvesting ***

            // Get string values for Elements to be stored as Strings
            // E.g. for Drugbank, read an indication text
            if(this.isStoredAsStr(this.getFieldName())){ // This is a field to be stored as String
                this.addText(this.getFieldName(), text);
            } else // Not to be stored as String
                // Get string values for Elements to be stored as Array of Strings
                // E.g. for Uniprot, read an interactant id or an accession number
                if(this.isStoredAsStrArray(this.getFieldName())){ // This is a field to be stored as String item in a JSONArray
                    tmpStr.append(text);// Strore the part of the id in the string buffer
                }

        } else {//XML formar error
            //Excecution shouldn't reach here - Outside entity but with text content
            if(!text.trim().equals("")){
                System.out.println("Warnig: XML format error :\n\t Entity named " + getFieldName() + " has 'text content' but its outside a \" " + basicElement + " \" Element. \n\t\ttext content: \"" + text +"\"" );
            }
            // ( if text is empty, the reason may be XML beautification with spaces which causes caracters() method to be called for XML Entities which are actually empty such as for MedlineCitation Entity itself" 
        }
    }

    /**
     * Nullifies entity-level variables to be reused
     *
     * @param uri
     * @param localName
     * @param qName
     * @throws SAXException
     */
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
//        System.out.println("     } //" + qName );
        if (this.getFieldName().equals(this.basicElement)){//exiting a entity Entity
            //Call process to index the document
            processor.process(this.getEntry());
//            System.out.println("entity instance nullified" );
            setObj(null);
        } else {//exiting an Entity except entity
            if(this.insideField(this.basicElement)){//exiting an Entity belonging to entity
                if(this.isStoredAsStrArray(this.getFieldName())){// Exiting an element that should be stored in a string array
                    this.addStringToJSONArray(this.getFieldName(),this.tmpStr.toString());// Strore the buffered String in the String list of the Entry Object
                    tmpStr.setLength(0); // reset tmpStr for further use
                } // Else not a field to be stored in an array of strings
            } else {//exiting an Entity not belonging to entity
                if (this.getFieldName().equals("DescriptorRecordSet")) {//The end of the process
                    System.out.println("\t *** DescriptorRecordSet Indexing End ***" );
                } else {
                    //XML formar error
                    //Excecution shouldn't reach here - not a vaid XML element of the ones listed below
                    System.out.println("XML format error :\n\t" + qName + " not a valid XML Element end. ");
                }
            }
        }
        //Nullify attributes variable
        this.attributes = null;
        endFieldName(qName);
    }

    /**
     * @return the mlcDoc
     */
    public JSONObject getEntry() {
        return entity;
    }

    /**
     * @param mlcDoc the mlcDoc to set
     */
    public void setObj(JSONObject mlcDoc) {
        this.entity = mlcDoc;
    }

    /**
     * @param qName the FieldName to check if in list of storedFields as Arrays
     * @return true if qName is a storedField, false otherwise
     */
    private boolean isStoredAsStrArray(String qName) {
        return this.storedAsStrArrays.contains(qName);
    }
    /**
     * @param qName the FieldName to check if in list of storedFields as Strings
     * @return true if qName is a storedField, false otherwise
     */
    private boolean isStoredAsStr(String qName) {
        return this.getStoredAsStr().contains(qName);
    }

    /**
     * Checks whether the current element is the field given or a child of it.
     *      i.e. If the filed given is currently open
     * @return true if should be ignored, false otherwise
     */
    private boolean insideField(String field) {
        return this.elements.contains(field);
    }

    /**
     * Sets the current element
     *      Takes into account renaming for conflicts (i.e. adding parent name in fieldName) 
     * @param qName the fieldName to add
     */
    public void startFieldName(String qName) {
//        System.out.println(this.elements);
        String parent = this.getFieldName(); // At this point the new element has not been added to the stack, its parent is the current element
        // Put the element in the stack of elements, using the parent-prefix if needed (i.e. if conflict)
        this.elements.push(handleConflicts(parent,qName));
    }

    /**
     * Removes the current element
     *      Takes into account renaming for conflicts (i.e. adding parent name in fieldName) 
     * @param qName the fieldName to set (as given by SAX Parser, without any prefix!)
     */
    public void endFieldName(String qName) {
//        System.out.println(this.elements);
        if(getFieldName().equals(handleConflicts(this.getParent(),qName))){
            this.elements.pop();
        } else {
            System.out.println(" " + new Date().toString() + " Error: " + qName + " trying to close, is not the current open Element " + getFieldName() );
        }
    }

    /**
     * Reads the current element (without remove)
     * @return the fieldName
     */
    public String getFieldName() {
        if(this.elements.isEmpty())
            return "ROOT";
        return this.elements.peek();
    }

    /**
     * Adds a text-value for given field in JSON object of the article
     *      If the field already has a text value, the new text is concatenated at the end. 
     *      If no text exists, it is just added.
     *
     *      ***
     *      IMPORTANT : The function relies in the assumption that field should only hold string values.
     *      ***
     *      IMPORTANT : "Empty" strings are ignored. 
     *      ***
     *
     * @param field     the filed to add value (fields used should only hold "string" values, no arrays etc)
     * @param text      the "text-value" to be added
     */
    public void addText(String field, String text){
        text = text.trim();
        if(!text.equals("")){ // notmal string value
            //        System.out.println(" addText for field " + field +  " : \"" + text + "\"");
            if(this.entity.keySet().contains(field)){ // field alredy exists, should have a text value
//            System.out.println(" \t existing : " + this.entity.get(field) );

                this.entity.put(field,this.entity.get(field) + text);
            } else {
                this.entity.put(field,text);
            }
            //        System.out.println(" \t new : " + this.entity.get(field) );
        } else { // empty string value
//            System.out.println(" \t\t Waring : empty string value ignored for field : \"" + field + "\"");                       
        }
    }

    /**
     * Adds a String item to a Json Array field of the JSON Object
     *  If not such a field exist, it is created 
     *
     *      ***
     *      IMPORTANT : The function relies in the assumption that field should only be a list of string values.
     *      ***
     *
     * @param field         String the name of the field to store the list e.g. "interactants" : []
     * @param interactant   String the id of the interactant to be added in the list
     */
    public void addStringToJSONArray(String field, String interactant){
//        System.out.println(" \t addStringToJSONArray Called field : " + field + " string : " + interactant + " entity : " + this.entity );
        JSONArray interactants = null;
        if(this.entity.keySet().contains(field)){ // Interactants list alredy exists, just add
//            System.out.println(" \t existing " + field + " elements : " + this.entity.get(field) );
            interactants = (JSONArray)this.entity.get(field);
        } else {
            interactants = new JSONArray();
        }
        interactants.add(interactant);
        this.entity.put(field,interactants);
    }

    /**
     * Checks whether this is a conflict FieldsName
     * @param fieldName      a FieldName (STring) to be checked for whether it is a conflict field
     * @return               true if current element is a ConflictFieldName, false otherwise
     *   In case this is a conflict element, it's parent field-name will be used as a prefix
     */
    private boolean isAConflictField(String fieldName) {
        return this.conflictFields.contains(fieldName);
    }

    /**
     * Get The parent of the current element
     *      Important, Works only AFTER the current element has been added to elements stack!
     * @return      The parent of the Parent element (i.e. the previous element in the stack of elements)
     */
    private String getParent(){
//        System.out.println(this.elements.size() + " * " + this.elements);
        if(this.elements.size() <= 1){ // Ths is the root element
            return "ROOT";
        } else {
            return this.elements.get(this.elements.size()-2);
        }
    }
    /**
     * Handles the naming of conflict elements (adding parent-prefix if needed)
     *      For any element in the set of conflictFields adds the parent-field as e prefix
     *      Exception, for the basic element (e.g. Entry for uniprot) no prefix is added (since prefixes are used for all the rest occurrences, no prefix indicates the basic entity)
     * @param fieldName
     * @return
     */
    private String handleConflicts(String parent, String fieldName){
        String finalFieldName = fieldName; // Initialize for the general case, where no prefix needed
        if(this.isAConflictField(fieldName)){ // This is a conflict Element (i.e. appears with parents of different type)
            if(!parent.equals(this.basicElement)){ // This is not a sub-element of basic entity, add the prefix
                finalFieldName = parent + FieldPrefixSeparator + fieldName;
            } // else, this is sub-element of the basic entity : Don't use a prefix - Conflict fields hold their initial name only for the basic element
        } // else, this is a normal Element (not a conflict) - no prefix needed

        return finalFieldName;
    }

    /**
     * Get the value of the attName attribute for the given field
     *      If no attributes exist all, or not the specific attribute exists, returns the empty string ("")
     * @param attName   The name of the attribute to retrieve
     * @return          The value of the attribute as a string (or "" instead of null, if no value exists)
     */
    private String getAttribute(String attName){
        String attValue = null;
        //Handle attributes
        if(attributes != null && attributes.getLength() > 0){// The entity has Attributes
            String currAttName = null;
            // For all attributes
            for(int i =0 ; i < attributes.getLength() ; i++){
                currAttName = attributes.getQName(i);
                if(currAttName != null && currAttName.equals(attName)){ // this is the requested attribute
                    attValue = attributes.getValue(i); // get the value
                }
            }
        }
        if (attValue == null)
            return "";
        else
            return attValue;
    }

    /**
     * @return the storedAsStr
     */
    public ArrayList <String> getStoredAsStr() {
        return storedAsStr;
    }
}
