package org.example.utils;

import org.example.RocketMQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
    public static void outputFile(String messageBody, String outputFilePath) {
        File outputFile = new File(outputFilePath);
        File outputDir = outputFile.getParentFile();
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                LOGGER.error("Error creating the output directory: {}", outputDir.toString());
            }
        }
        if (!outputFile.exists()) {
            try {
                outputFile.createNewFile();
            } catch (IOException e) {
                LOGGER.error("Error creating the output file: {}", outputFilePath, e);
            }
        }
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            fos.write(messageBody.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("Error writing message body to file: {}", outputFilePath, e);
        }
    }

    public static String readFromFile(String fileName) {
        StringBuilder content = new StringBuilder();

        try (InputStream inputStream = FileUtils.class.getClassLoader().getResourceAsStream(fileName);
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                content.append(line);
                content.append(System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content.toString();
    }

}
