/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package help;


import DiffHarvester.DatasetCreator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
// Add json-simple-1.1.1.jar
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 *
 * @author tasosnent
 *
 * Various Helper methods
 *
 */
public class Helper {

    /**
     * Convert Array of Strings into a JSONArray of Strings
     * @param StrArr    Array of Strings
     * @return          corresponding JSONArray of Strings
     */
    public static JSONArray StringArrayToJSONList(String[] StrArr){
        JSONArray List = new JSONArray();
        for(int i=0 ; i < StrArr.length; i++){
            List.add(StrArr[i]);
        }
        return List;
    }

    /**
     * Convert Array of Strings into a String
     *      Used to concatenate Abstract sections into an abstract string for PubMed articles
     *
     * @param StrArr    Array of Strings
     * @return          String of concatenated Strings
     */
    public static String StringArrayToString(String[] StrArr){
        String str = new String();
        for(int i=0 ; i < StrArr.length; i++){
            str += StrArr[i];
        }
        return str;
    }

    /**
     * Convert a String ArrayList to a String Array
     * @param ArrList   The given String ArrayList
     * @return          The conversion of the given ArrayList to Array
     */

    public static String[] ArrayListToArray(ArrayList<String> ArrList){
        String[] array = new String[ArrList.size()];
        for(int i=0; i<=ArrList.size()-1; i++){
            array[i]=ArrList.get(i);
        }
        return array;
    }

    /**
     * Convert a string containing comma-separated words to ArrayList
     * @param string The string containing comma-separated words
     * @return       ArrayList with separated words
     */
    public static ArrayList<String> StringToArrayList(String string){
        ArrayList<String> AL = new ArrayList<String>(Arrays.asList(string.split(",")));
        return AL;
    }

    /**
     * Remove duplicates from an array list
     * @param ArrList   The string ArrayList
     * @return          The String ArrayList without duplicates
     */
    public static ArrayList<String> removeDublicatesFromArrayList(ArrayList<String> ArrList){
        ArrayList<String> unique = new ArrayList<>();
        for(String s : ArrList){
            if(!unique.contains(s)){
                unique.add(s);
            }
        }
        return unique;
    }

    /**
     * Prints a message of time passed for a job
     *
     * @param start Date object before the job
     * @param end   Date object after the job
     * @param job   a String describing the job
     */
    public static void printTime(Date start, Date end, String job){
         long miliseconds = end.getTime() - start.getTime();
                String totalTime = String.format("%02d:%02d:%02d", 
                    TimeUnit.MILLISECONDS.toHours(miliseconds),
                    TimeUnit.MILLISECONDS.toMinutes(miliseconds) - 
                    TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(miliseconds)),
                    TimeUnit.MILLISECONDS.toSeconds(miliseconds) - 
                    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(miliseconds)));
                //log printing
                System.out.println(" " + new Date().toString() + " Total " + job + " time : " + totalTime); 
    }

    /**
     * Create a folder in the specified path
     *
     * @param folder Path to create the folder
     * @return  true if folder created, false otherwise
     */
    public static boolean createFolder(String folder){
        if (!(new File(folder)).mkdirs()) {
            printMessage("Warning!: Folder "+ folder +" allready Exists",2);
            return false;
        } else {
//            printMessage(" Folder "+ folder +" succesfully created");
            return true;
        }
    }

    /**
     * Given the paths of the 2 files (1st file (oldFile): file with the desired name
     *                                 2nd file (newTempFile): file to rename)
     * rename the temp file with the desired name and delete unwanted files.
     * @param oldFile   path to the file that have the name we want to rename to.
     * @param tempfile  path to the file that we want to rename
     */
    public static void renameFile(String oldFile, String tempfile){
        File inputFile = new File(oldFile);
        File tempFile = new File(tempfile);
        inputFile.delete();
        tempFile.renameTo(inputFile);
    }

    /**
     * It returns the file size in MegaBytes (bytes/(1024*1024)
     * @param filepath  path to file we want to get the size
     * @return          The file size as String
     */
    public static String getFileSizeInMB(String filepath){
        File File = new File(filepath);
        String size = String.valueOf((File.length())/1048576);

        return size;
    }

    /** print a message in specified depth
     *
     * @param msg       String message
     * @param depth     number of tabs to intend
     */
    public static void printMessage(String msg, int depth ){
        for(int i = 0; i < depth; i++)
            System.out.print("\t");
        System.out.println(msg);
    }

    /**
     * return "JSONArray of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      JSONArray found or null
     */
    public static JSONArray getJSONArray(String key, JSONObject o){
        JSONArray a = null;
        if(o.containsKey(key)){
            if(o.get(key) instanceof JSONArray){
                a = (JSONArray) o.get(key);
            } else {
//                printMessage("Warning: element with key "+key+" not a valid JSONArray. in " + o.toJSONString());               
            }
        }// else { printMessage("Error: element with key "+key+" not present in " + o.toJSONString());}
        return a;
    }


    /** Return "JSONObject of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      JSONObject found or null
     */
    public static JSONObject getJSONObject(String key, JSONObject o){
        JSONObject a = null;
        if(o.containsKey(key)){
            if(o.get(key) instanceof JSONObject){
                a = (JSONObject) o.get(key);
            } else {
                printMessage("Warning: element with key "+key+" not a valid JSONObject. in " + o.toJSONString(),2);
            }
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return a;
    }

    /** Return "String of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      String found or null
     */
    public static String getString(String key, JSONObject o){
        String a = null;
        if(o.containsKey(key)){
            if(o.get(key) instanceof String){
                a = (String) o.get(key);
            } else {
                printMessage("Warning: element with key "+key+" not a valid String. in " + o.toJSONString(),2);
            }
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return a;
    }

    /**
     * Return "Integer value of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      Integer found or null
     */
    public static Integer getInteger(String key, JSONObject o){
        if(o.containsKey(key)){
            if(o.get(key) instanceof Integer){
                int a = (Integer) o.get(key);
                return a;
            } else {
                printMessage("Warning: element with key "+key+" not a valid int. in " + o.toJSONString(),2);
            }
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return null;
    }

    /** Return "Double value of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      Double found or null
     */
    public static Double getDouble(String key, JSONObject o){
        if(o.containsKey(key)){
            if(o.get(key) instanceof Double){
                double a = (Double) o.get(key);
                return a;
            } else {
                printMessage("Warning: element with key "+key+" not a valid Double. in " + o.toJSONString(),2);
            }
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return null;
    }

    /** Return "String value of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      String found or null
     */
    public static String getAnyType(String key, JSONObject o){
        if(o.containsKey(key)){
            return o.get(key)+"";
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return null;
    }

    /**
     * Return "Float value of key provided" or null, in case of absence or wrong type
     *
     * @param key   Name of field to search
     * @param o     JSONObject to search in 
     * @return      Float found or null
     */
    public static Float getFloat(String key, JSONObject o){
        if(o.containsKey(key)){
            if(o.get(key) instanceof Float){
                float a = (Float) o.get(key);
                return a;
            } else {
                printMessage("Warning: element with key "+key+" not a valid float. in " + o.toJSONString(),2);
            }
        } //else {  printMessage("Warning: element with key "+key+" not present in " + o.toJSONString()); }
        return null;
    }

}
