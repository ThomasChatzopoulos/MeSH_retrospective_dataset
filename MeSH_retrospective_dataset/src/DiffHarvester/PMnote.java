/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DiffHarvester;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 *  Representing a Pubic MeSH Note field of a MeSH descriptor
 * @author tasosnent
 */
public class PMnote {
    private String note; // The PMnotecontent of a Descriptor
    // E.g. "2006; HEAT-SHOCK PROTEIN 47 was indexed under HEAT-SHOCK PROTEINS 1993-2005"
    // E.g. "2013; RHO-GDIALPHA was indexed under GUANINE NUCLEOTIDE DISSOCIATION INHIBITORS 2011-2012, RHO GUANINE NUCLEOTIDE DISSOCIATION INHIBITOR 1 was indexed under GUANINE NUCLEOTIDE DISSOCIATION INHIBITORS 2012"
    private static Pattern scrPatern = Pattern.compile(";?([^;]*) was indexed under ([^;]*);?"); // Pattern to exploit the "X was indexed under y" rule for recognizing the previous SCR
    private static String patternQuery = "was indexed under"; // A string to search in PMnote for identifying pttern amenable cases 
    private Matcher matcher; // A mather to use the scrPatern
    private String scrTerm = null;
    private String dNames = null;
    private boolean patternAmenable = true;

    /**
     *  Constructor for a PMnote object
     * @param note
     */
    public PMnote (String note){
        this.note = note;
        if(note != null && note.contains(patternQuery)){
            Matcher matcher = scrPatern.matcher(note);
            if(matcher.find()){
//            System.out.println("\t\t(0) \"" + matcher.group(0) +"\"");
//            System.out.println("\t\t(1) \"" + matcher.group(1) +"\"");
                String termRaw = matcher.group(1);
                if(termRaw.contains("(now")){
                    // Handle cases like: NK Cell Lectin-Like Receptor Subfamily K(D055655) with PMnote: 2009; NKG2D RECEPTOR (now KILLER CELL LECTIN-LIKE RECEPTOR SUBFAMILY K) was indexed under RECEPTORS, IMMUNOLOGIC 2005-2008, under RECEPTORS MITOGEN 1993-2004, and under UNDER MEMBRANE GLYCOPROTEINS 1993-1998
                    String[] parts = termRaw.split("\\(now");
                    termRaw = parts[0];
                    //                System.out.println("\t\t(1') \"" +termRaw +"\"");
                } else if(termRaw.contains("now")){
                    // Handle cases like: SMN Complex Proteins(D055532) with PMnote: 2009; SMN PROTEIN (SPINAL MUSCULAR ATROPHY) now SMN COMPLEX PROTEINS was indexed under NERVE TISSUE PROTEINS, RNA-BINDING PROTEINS,&CYCLIC AMP RESPONSE ELEMENT-BINDING PROTEIN 1995-2008
                    String[] parts = termRaw.split("now");
                    termRaw = parts[0];
                }
                termRaw = termRaw.trim();
                if(!termRaw.equals("")){
                    this.scrTerm = termRaw;
                }
                this.dNames =  matcher.group(2);
            }
        } else {
//            System.out.println("PMnote not covered by the \"...was indexed under...\" patern");
            this.patternAmenable = false;
        }

    }
    @Override
    public String toString(){
        String s = "";
        if(patternAmenable){
            s = "X -("+this.patternCount()+")> Y :" + scrTerm + " -> " +dNames ;
        } else if(note != null){
            s = " Original Note :" + note;
        } else {
            s = " Empty PMnote object";
        }
        return s;
    }
    /**
     * @return the note
     */
    public String getNote() {
        return note;
    }

    /**
     * @param note the note to set
     */
    public void setNote(String note) {
        this.note = note;
    }

    /**
     * Whether this object corresponds to an descriptor with empty PMfield
     * @return
     */
    public boolean isEmpty(){
        if(getNote() != null){
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return the scrTerm
     */
    public String getScrTerm() {
        return scrTerm;
    }

    /**
     * @param scrTerm the scrTerm to set
     */
    public void setScrTerm(String scrTerm) {
        this.scrTerm = scrTerm;
    }

    /**
     * @return the dNames
     */
    public String getdNames() {
        return dNames;
    }

    /**
     * @param dNames the dNames to set
     */
    public void setdNames(String dNames) {
        this.dNames = dNames;
    }

    /**
     * Whether the PMnote of this descriptor is amenable to the pattern for SCR extraction
     * @return the patternAmenable
     */
    public boolean patternAmenable() {
        return patternAmenable;
    }

    /**
     * Whether an scrTerm has been extracted by the pattern
     * @return
     */
    public boolean termFound(){
        if(this.scrTerm !=null){
            return true;
        }
        return false;
    }

    /**
     *
     * @return
     */
    public int patternCount(){
        int count = 0;
        if(this.patternAmenable){
            count = StringUtils.countMatches(this.note, patternQuery);
        }
        return count;
    }

}
