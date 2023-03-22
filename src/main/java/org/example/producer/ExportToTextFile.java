package org.example.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExportToTextFile {
    private static final Logger LOGGER = Logger.getLogger(ExportToTextFile.class.getName());

    public static void writeStringToFile(String content, String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(content);
            LOGGER.log(Level.INFO, "Content written to file: {0}", filePath);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error writing content to file: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        String sql = "SELECT * FROM test_innodb";
        try {
            String resourcesPath = "src/main/resources/";
//            String resourcesPath = resourceUrl.getPath();
            String outputFilePath = resourcesPath + "input.txt";
            String resultSetString = DatabaseUtils.executeQuery(sql,"jdbc:mysql://172.16.2.74:3306/helloJiu","root",
                    "QH)qb}GR7EA*");
            writeStringToFile(resultSetString, outputFilePath);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error executing query and exporting to text file: " + e.getMessage(), e);
        }
    }
}
