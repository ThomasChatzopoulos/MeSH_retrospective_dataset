/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yamlSettings;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
//add snakeyaml-1.18.jar
import org.yaml.snakeyaml.Yaml;

/**
 * Parses YAML settings 
 *      Load the YAML file of settings recursively into corresponding structures (A hierarchy of HashMaps with leaf values of primitive types)
 *      Provide functions to read and update a setting given it's "path" in the YAML file structure.
 *
 * @author tasosnent
 *
 */
public class Settings {
    private static Yaml yaml = new Yaml();

    private String settingsFilePath;
    // Settings hierarchy of HashMaps
    HashMap settings;

    /**
     * Constructor
     * @param filePath  path to the YAML file containing settings
     */
    public Settings(String filePath){
        this.settingsFilePath = filePath;

        //Load yaml
        try {
            String settingsFile;
            settings = (HashMap)yaml.load(new FileInputStream(new File(settingsFilePath)));
            System.out.println("Running with settings : " + settings);
//            settings = (HashMap)read(settings);
//            this.saveSettings();

        } catch (FileNotFoundException ex) {
            System.out.println(ex.getLocalizedMessage());
        }

    }

    /**
     * Save the (edited?) Settings, updating the settings file.
     */
    public void saveSettings(){
        writeFile(settingsFilePath, yaml.dump(settings));
    }
    /**
     * Set the value of a property in the YAML file
     * @param propertyPath  The path in YAML file to find the string value of the property (e.g. "pipeline/in/inp")
     * @param value         String value of the property to set
     */
    public void setProperty(String propertyPath,Object value){
//        System.out.println(" setString " + propertyPath +", " + value);

        String[] levels = propertyPath.split("/");
        HashMap currentLevel = settings;
        for(int i=0 ; i < levels.length-1 ; i++ ){
//                    System.out.println(" -- "+ i + " " + (levels[i]) + " " +currentLevel);
            if(currentLevel.containsKey(levels[i])){
                currentLevel.put(levels[i], currentLevel.get(levels[i]));
                currentLevel=(HashMap)currentLevel.get(levels[i]);
            } else {
                System.out.println(" Error writing settings : No \"" + (levels[i]) + "\" field found in " + currentLevel);
            }
        }
//                    System.out.println(" --- " + (levels[levels.length-1]) + " " + currentLevel);
        currentLevel.put(levels[levels.length-1], value);
        saveSettings();
    }

    /**
     * Get the value of a property in the YAML file
     * @param propertyPath  The path in YAML file to find the string value of the property (e.g. "pipeline/in/inp")
     * @return              String value of the property
     */
    public Object getProperty(String propertyPath){
//        System.out.println(" getString " + propertyPath);

        String[] levels = propertyPath.split("/");
        HashMap currentLevel = settings;
        for(int i=0 ; i < levels.length-1 ; i++ ){
//            System.out.println(" level  " + i + " " + currentLevel);
            if(currentLevel.containsKey(levels[i])){
                currentLevel=(HashMap)currentLevel.get(levels[i]);
            } else {
                System.out.println(" Error reading settings : No \"" + (levels[i]) + "\" field found in " + currentLevel);
            }
        }
//            System.out.println(" level  " + (levels.length-1) + " " + currentLevel );
        return currentLevel.get(levels[levels.length-1]);
    }

    /**
     * Write a String into a file
     *
     * @param filePath      Path to write the file
     * @param content       String value to be written
     */
    public static void writeFile(String filePath, String content){
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
//            jsonWriter.append(subbmission.toJSONString());
            // print human readable using GsonBuilder library
            // disableHtmlEscaping to not replace sepcial characters
            writer.append(content);
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            System.out.println(" " + new Date().toString() + " caught a IOException writing in file " + filePath);
            ex.printStackTrace();
        }
    }

}
