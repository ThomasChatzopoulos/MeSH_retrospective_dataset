/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DiffHarvester;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONArray;

/**
 * Representing a Previous Indexing Field of a MeSH Descriptor
 * @author tasosnent
 */
public class PreviousIndexing {
    private String DesriptorName = null;
    private String yearFrom = null;
    private String yearTo = null;
    private String qualifierName = null;

    /**
     *  Initializing a PreviousIndexing Object by the textual content of a PI field
     * @param s the textual content of a PI field
     */
    public PreviousIndexing(String s){
//      Examples:   specific muscles/surgery (1968-2010)
//                  Dapsone/analogs & derivatives (1975) 
//                  specific artery
//                  Utilization Review (1968-Jul 1977)
//                  Endorphins (Jul 1977-1984) 
//                  Food Services (Jan-Jul 1977)

        // Hardcoded expections not catched by the regular expressions
        HashMap <String, String> exceptions = new HashMap <String, String> ();
        exceptions.put("(Jan-Jul 1977)", "1977"); // yearParentheses -> yearTo
        exceptions.put("\"( 66-1974)\"", "1974"); // yearParentheses -> yearTo
        exceptions.put("(1979-82,89-1992)", "1992"); // yearParentheses -> yearTo

        Pattern basicPatern = Pattern.compile("([^\\(\\)/]+?)(/([^\\(\\)/]*))?( *\\(.*?\\))?$");
        Pattern yearPatern = Pattern.compile("\\((\\w{3} )?(\\d*)(-(\\w{3} )?(\\d*))?\\)$");
//        Pattern piPatern = Pattern.compile("([^\\(\\)/]+?)(/(.*))?( *\\((\\w{3} )?(\\d*)(-(\\w{3} )?(\\d*))?\\))?$"); // 
        Matcher matcher = basicPatern.matcher(s);
//         System.out.println(s);
        if (matcher.find()) {// Valid previous indexing found
//            for(int i=0 ; i<=matcher.groupCount(); i++){                
//                System.out.println( " " + i + " >"+matcher.group(i));
//            }
            DesriptorName = matcher.group(1);
            if(DesriptorName!= null){
                DesriptorName = DesriptorName.trim();
            }
            qualifierName = matcher.group(3);
            if(qualifierName!= null){
                qualifierName = qualifierName.trim();
            }
            String yearParentheses = matcher.group(4);
            if(yearParentheses != null){
                matcher = yearPatern.matcher(yearParentheses);
                if (matcher.find()) {// Valid previous indexing found
//                    for(int i=0 ; i<=matcher.groupCount(); i++){                
//                        System.out.println( " " + i + " >"+matcher.group(i));
//                    }
                    yearFrom = matcher.group(2);

                    yearTo = matcher.group(5);
                } else {
                    System.out.println("\t Warning : This srting doesn't match the previous Indexing pattern for years: " + yearParentheses);
                    // Hardoced exceptions considered
                    for(String key : exceptions.keySet()){
                        if(yearParentheses.contains(key)){
                            yearTo =  exceptions.get(key);
                            System.out.println("\t\t\t Exception match used: yearTo =" + yearTo);
                        }
                    }
                }
            } // else no year info
//            System.out.println(this);
        } else {
            System.out.println("\t Warning : This srting doesn't match the basic previous Indexing pattern: " + s);
        }
//         printForChecking(s);
    }

    /**
     *
     * @param sl
     * @return
     */
    public static ArrayList<PreviousIndexing> readPIList(JSONArray sl){
        ArrayList<PreviousIndexing> piList = new ArrayList<PreviousIndexing> ();
        for(Object s: sl){
            piList.add(new PreviousIndexing((String)s));
        }
        return piList;
    }
    /**
     * Print the extracted PI list for estimating the correctness of the extracted information
     * @param s
     */
    public void printForChecking(String s){
        System.out.println( "PI note: " + s);
        System.out.println( "PI object: " + this);
    }

    @Override
    public String toString(){
        String s = "";
        String q = "";
        if(getQualifierName() != null){
            q = "/" + getQualifierName();
        }
        s = getDesriptorName() + q + " (" + getYearFrom() + "-" + getYearTo() + ")";
        return s;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args){
        PreviousIndexing pi = new PreviousIndexing("specific muscles/surgery (1968-2010)");
        System.out.println(pi);
        pi = new PreviousIndexing("specific muscles (1968-2010)");
        System.out.println(pi);
    }

    /**
     * @return the DesriptorName
     */
    public String getDesriptorName() {
        return DesriptorName;
    }

    /**
     * @param DesriptorName the DesriptorName to set
     */
    public void setDesriptorName(String DesriptorName) {
        this.DesriptorName = DesriptorName;
    }

    /**
     * @return the yearFrom
     */
    public String getYearFrom() {
        return yearFrom;
    }

    /**
     * @param yearFrom the yearFrom to set
     */
    public void setYearFrom(String yearFrom) {
        this.yearFrom = yearFrom;
    }

    /**
     * @return the yearTo
     */
    public String getYearTo() {
        return yearTo;
    }

    /**
     * @param yearTo the yearTo to set
     */
    public void setYearTo(String yearTo) {
        this.yearTo = yearTo;
    }

    /**
     * @return the qualifierName
     */
    public String getQualifierName() {
        return qualifierName;
    }

    /**
     * @param qualifierName the qualifierName to set
     */
    public void setQualifierName(String qualifierName) {
        this.qualifierName = qualifierName;
    }
}
