package com.bangor.utils;

import java.io.*;
import java.util.*;

/**
 * Created by Joseph W Plant on 16/11/2014.
 */
public class UtilityFiles {

    /**
     * reads all files in the given directory
     *
     * @param dir
     * @return
     * @throws IOException
     */
    public static String[] readDirLineByLine(File dir) throws IOException {
        ArrayList<String> data = new ArrayList<String>();
        if (dir.isDirectory()) {
            File[] fileArr = dir.listFiles();
            for (File file : fileArr) {
                String[] fileData = UtilityFiles.readLineByLine(file);
                List<String> stringList = Arrays.asList(fileData);
                data.addAll(stringList);
            }
        }

        return data.toArray(new String[]{});
    }

    /**
     * reads a file line by line into a String[]
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static String[] readLineByLine(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        ArrayList<String> lines = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }
        br.close();

        return lines.toArray(new String[0]);
    }

    /**
     * writes the map to the file key[TAB]value
     *
     * @param map
     * @param outputFile
     */
    public static void writeLineByLine(HashMap<Integer, Integer> map, File outputFile) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
        bw.write("");
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            bw.write(key);
            bw.write("\t");
            bw.write(value);
            bw.newLine();
        }
        bw.close();
    }
}
