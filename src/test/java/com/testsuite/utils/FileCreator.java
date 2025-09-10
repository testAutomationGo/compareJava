package com.testsuite.utils;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import javax.imageio.ImageIO;

import com.lowagie.text.Document;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfWriter;

public class FileCreator {
    private static final Random R = new Random();
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS");

    public static String createTextFile(String folderPath, String tcNumber) {
        String fileName = "Text_" + tcNumber + "_" + now() + ".txt";
        Path p = Path.of(folderPath, fileName);
        writeString(p, "TC Number: " + tcNumber + ". This is a text file. " + randAlNum(10) + randNum(5) + now());
        return fileName;
    }

    public static String createTextFileSizeInKB(String folderPath, String tcNumber, int sizeInKB) {
        String fileName = "Text_" + tcNumber + "_" + now() + ".txt";
        Path p = Path.of(folderPath, fileName);
        int chunk = 1024;
        String base = baseText(tcNumber);
        int fill = Math.max(0, chunk - base.getBytes(StandardCharsets.UTF_8).length);
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(p))) {
            for (int i = 0; i < sizeInKB; i++) {
                bos.write(base.getBytes(StandardCharsets.UTF_8));
                if (fill > 0) bos.write(randBytes(fill));
            }
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createTextFileSizeInMB(String folderPath, String tcNumber, int sizeInMB) {
        return createTextFileSizeInKB(folderPath, tcNumber, sizeInMB * 1024);
    }

    public static String createTextFileWithSpecificText(String folderPath, String tcNumber, String text) {
        String fileName = "Text_" + tcNumber + "_" + now() + ".txt";
        writeString(Path.of(folderPath, fileName), text);
        return fileName;
    }

    public static String createTextFileWithSpecificFileNameSize(String folderPath, String tcNumber, int size) {
        int capped = Math.min(size, 239);
        String fileName = "Text_" + tcNumber + "_" + lowerAlpha(capped) + ".txt";
        Path p = Path.of(folderPath, fileName);
        writeString(p, "TC Number: " + tcNumber + ". This is a text file. " + randAlNum(10) + randNum(5) + now());
        return fileName;
    }

    public static void createTextFileWithSpecificFileName(String folderPath, String fileName) {
        Path p = Path.of(folderPath, fileName);
        try { Files.createDirectories(p.getParent()); Files.write(p, new byte[0]); } catch (IOException ignored) {}
    }

    public static String createJSONFile(String folderPath, String tcNumber) {
        String fileName = "JSON_" + tcNumber + "_" + now() + ".json";
        String json = """
                {
                  "type": "JSON File",
                  "createdAt": "%s",
                  "message": "%s Hello, this is a json file.",
                  "data": "%s%s"
                }
                """.formatted(now(), tcNumber, randAlNum(10), randNum(5));
        writeString(Path.of(folderPath, fileName), json);
        return fileName;
    }

    public static String createJSONFileInKB(String folderPath, String tcNumber, int sizeInKB) {
        if (sizeInKB < 1) sizeInKB = 1;
        if (sizeInKB > 1024) sizeInKB = 1024;
        
        String fileName = "JSON_" + tcNumber + "_" + now() + ".json";
        Path p = Path.of(folderPath, fileName);
        
        try (BufferedWriter writer = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            writer.write("{\n");
            writer.write("  \"type\": \"JSON File\",\n");
            writer.write("  \"createdAt\": \"" + now() + "\",\n");
            writer.write("  \"message\": \"" + tcNumber + " Hello, this is a json file.\",\n");
            writer.write("  \"data\": \"" + randAlNum(10) + randNum(5) + "\",\n");
            writer.write("  \"tags\": {\n");
            
            for (int i = 0; i < sizeInKB; i++) {
                boolean isLast = i == sizeInKB - 1;
                String prefix = String.format("    \"tag%04d\": \"", i + 1);
                String suffix = "\"" + (isLast ? "" : ",") + "\n";
                int fillLen = Math.max(0, 1024 - prefix.length() - suffix.length());
                String value = randAlNum(fillLen);
                writer.write(prefix + value + suffix);
            }
            
            writer.write("  }\n");
            writer.write("}\n");
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createJSONFileFromString(String folderPath, String tcNumber, String jsonString) {
        String fileName = "JSON_" + tcNumber + "_" + now() + ".json";
        writeString(Path.of(folderPath, fileName), jsonString);
        return fileName;
    }

    public static String createJPEGFile(int w, int h, String folderPath, String tcNumber) {
        String fileName = "JPEG_" + tcNumber + "_" + now() + ".jpeg";
        writeImage(folderPath, fileName, w, h, "jpeg");
        return fileName;
    }

    public static String createJPGFile(int w, int h, String folderPath, String tcNumber) {
        String fileName = "JPG_" + tcNumber + "_" + now() + ".jpg";
        writeImage(folderPath, fileName, w, h, "jpg");
        return fileName;
    }

    public static String createPNGFile(int w, int h, String folderPath, String tcNumber) {
        String fileName = "PNG_" + tcNumber + "_" + now() + ".png";
        writeImage(folderPath, fileName, w, h, "png");
        return fileName;
    }

    public static String createPDFFile(String folderPath, String tcNumber) {
        String fileName = "PDF_" + tcNumber + "_" + now() + ".pdf";
        Path p = Path.of(folderPath, fileName);
        try (OutputStream out = Files.newOutputStream(p);
             Document doc = new Document()) {
            PdfWriter.getInstance(doc, out);
            doc.open();
            String content = tcNumber + " This is a PDF file. " + now() + " " + randAlNum(10) + randNum(5);
            doc.add(new Paragraph(content));
        } catch (Exception ignored) {}
        return fileName;
    }

    public static String createPDFFileInKB(String folderPath, String tcNumber, int sizeInKB) {
        if (sizeInKB < 1) sizeInKB = 1;
        if (sizeInKB > 1024) sizeInKB = 1024;
        
        String fileName = "PDF_" + tcNumber + "_" + now() + ".pdf";
        Path p = Path.of(folderPath, fileName);
        try (OutputStream out = Files.newOutputStream(p);
             Document doc = new Document()) {
            PdfWriter.getInstance(doc, out);
            doc.open();
            String content = tcNumber + " This is a PDF file. " + now() + "\n" + randAlNum(15) + randNum(5);
            int linesNeeded = (sizeInKB * 1024) / content.length();
            for (int i = 0; i < linesNeeded * 4; i++) {
                doc.add(new Paragraph(content));
            }
        } catch (Exception ignored) {}
        return fileName;
    }

    public static String createHTMLFile(String folderPath, String tcNumber) {
        String fileName = "HTML_" + tcNumber + "_" + now() + ".html";
        String html = """
        <html>
        <div id="text">%s This is some html version_%s %s%s</div>
        <button id="button">push</button>
        <script>
        var button=document.getElementById("button");
        var text=document.getElementById("text");
        button.addEventListener("click",function(){if(text.style.color==="red"){text.style.color="green";}else{text.style.color="red";}});
        </script>
        </html>
        """.formatted(tcNumber, now(), randAlNum(10), randNum(5));
        writeString(Path.of(folderPath, fileName), html);
        return fileName;
    }

    public static String createHTMLFileInKB(String folderPath, String tcNumber, int sizeInKB) {
        if (sizeInKB < 1) sizeInKB = 1;
        if (sizeInKB > 1024) sizeInKB = 1024;
        
        String fileName = "HTML_" + tcNumber + "_" + now() + ".html";
        Path p = Path.of(folderPath, fileName);
        
        try (BufferedWriter writer = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            String currentTime = now();
            writer.write("<html>\n");
            writer.write("<div id=\"text\">" + tcNumber + " This is some html version_" + currentTime + " " + randAlNum(10) + randNum(5) + "</div>\n");
            writer.write("<button id=\"button\">push</button>\n");
            writer.write("<script>\n");
            writer.write("var button = document.getElementById(\"button\");\n");
            writer.write("var text = document.getElementById(\"text\");\n");
            writer.write("button.addEventListener(\"click\", function() {\n");
            writer.write("  if (text.style.color == \"red\") {\n");
            writer.write("    text.style.color = \"green\";\n");
            writer.write("  } else {\n");
            writer.write("    text.style.color = \"red\";\n");
            writer.write("  }\n");
            writer.write("});\n");
            writer.write("</script>\n");
            writer.write("</html>\n");
            
            for (int i = 0; i < sizeInKB; i++) {
                String content = randAlNum(65) + "\n";
                writer.write(content);
            }
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createXMLFile(String folderPath, String tcNumber) {
        String fileName = "XML_" + tcNumber + "_" + now() + ".xml";
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <note>
                  <to>User</to>
                  <from>Tester</from>
                  <heading>Reminder</heading>
                  <body>%s Don't forget to check this XML file! %s%s</body>
                </note>
                """.formatted(tcNumber, randAlNum(10), randNum(5));
        writeString(Path.of(folderPath, fileName), xml);
        return fileName;
    }

    public static String createXMLFileInKB(String folderPath, String tcNumber, int sizeInKB) {
        if (sizeInKB < 1) sizeInKB = 1;
        if (sizeInKB > 1024) sizeInKB = 1024;
        
        String fileName = "XML_" + tcNumber + "_" + now() + ".xml";
        Path p = Path.of(folderPath, fileName);
        
        try (BufferedWriter writer = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            writer.write("<note>\n");
            writer.write("  <to>User</to>\n");
            writer.write("  <from>Tester</from>\n");
            writer.write("  <heading>Reminder</heading>\n");
            writer.write("  <body>" + tcNumber + " Don't forget to check this XML file! " + randAlNum(10) + randNum(5) + "</body>\n");
            writer.write("  <tags>\n");
            
            for (int i = 0; i < sizeInKB; i++) {
                boolean isLast = i == sizeInKB - 1;
                String prefix = String.format("    <tag%04d>", i + 1);
                String suffix = String.format("</tag%04d>", i + 1) + (isLast ? "" : "") + "\n";
                int fillLen = Math.max(0, 1024 - prefix.length() - suffix.length());
                String value = randAlNum(fillLen);
                writer.write(prefix + value + suffix);
            }
            
            writer.write("  </tags>\n");
            writer.write("</note>\n");
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createDockerFile(String folderPath, String tcNumber) {
        String fileName = "Dockerfile_" + tcNumber + "_" + now() + ".dockerfile";
        String s = """
            FROM maven:3.9.8-eclipse-temurin-21 AS build
            WORKDIR /app
            COPY . .
            RUN mvn -q -DskipTests package
            FROM eclipse-temurin:21-jre
            WORKDIR /app
            COPY --from=build /app/target/*.jar app.jar
            EXPOSE 8080
            ENTRYPOINT ["java","-jar","/app/app.jar"]
            """;
        writeString(Path.of(folderPath, fileName), s);
        return fileName;
    }

    public static String createYMLFile(String folderPath, String tcNumber) {
        String fileName = "YML_" + tcNumber + "_" + now() + ".yml";
        String s = """
            version: '3.8'
            services:
              app:
                image: eclipse-temurin:21-jre
                working_dir: /app
                volumes:
                  - ./:/app
                command: ["java","-jar","app.jar"]
                ports:
                  - "8080:8080"
                  """;
        writeString(Path.of(folderPath, fileName), s);
        return fileName;
    }

    public static String createCSVFile(String folderPath, String tcNumber) {
        String fileName = "CSV_" + tcNumber + "_" + now() + ".csv";
        Path p = Path.of(folderPath, fileName);
        try (BufferedWriter w = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            w.write("ID,Name,Email\n");
            for (int i = 1; i <= 10; i++) w.write(i + ",User" + i + ",user" + i + "@example.com\n");
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createCSVFileInKB(String folderPath, String tcNumber, int sizeInKB) {
        if (sizeInKB < 1) sizeInKB = 1;
        if (sizeInKB > 1024) sizeInKB = 1024;
        
        String fileName = "CSV_" + tcNumber + "_" + now() + ".csv";
        Path p = Path.of(folderPath, fileName);
        
        try (BufferedWriter w = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            w.write("ID,Name,Email\n");
            for (int i = 2; i <= sizeInKB; i++) {
                w.write(i + ",User" + i + ",user" + i + "@example.com," + randAlNum(999) + randNum(20) + "\n");
            }
        } catch (IOException ignored) {}
        return fileName;
    }

    public static String createJavaFile(String fileContents, String folderPath, String tcNumber) {
        String fileName = "Java_" + tcNumber + "_" + now() + ".java";
        writeString(Path.of(folderPath, fileName), fileContents);
        return fileName;
    }

    public static String createGoFile(String fileContents, String folderPath, String tcNumber) {
        String fileName = "Go_" + tcNumber + "_" + now() + ".go";
        writeString(Path.of(folderPath, fileName), fileContents);
        return fileName;
    }

    public static String createFileByType(String fileType, String folderPath, String tcNumber) {
        String t = fileType == null ? "" : fileType.toLowerCase();
        return switch (t) {
            case "txt" -> createTextFile(folderPath, tcNumber);
            case "json" -> createJSONFile(folderPath, tcNumber);
            case "jpeg" -> createJPEGFile(100, 100, folderPath, tcNumber);
            case "jpg" -> createJPGFile(100, 100, folderPath, tcNumber);
            case "png" -> createPNGFile(100, 100, folderPath, tcNumber);
            case "pdf" -> createPDFFile(folderPath, tcNumber);
            case "html" -> createHTMLFile(folderPath, tcNumber);
            default -> "";
        };
    }

    public static String createAFolderWith3TextFiles(String folderPath, String tcNumber) {
        String folderName = tcNumber + "_" + now();
        Path root = Path.of(folderPath, folderName);
        try { Files.createDirectories(root); } catch (IOException ignored) {}
        for (int i = 0; i < 3; i++) createTextFile(root.toString(), tcNumber + i);
        return folderName;
    }

    public static String createAFolderWithXNumberOfTextFiles(String folderPath, int x, String tcNumber) {
        String folderName = tcNumber + "_" + now();
        Path root = Path.of(folderPath, folderName);
        try { Files.createDirectories(root); } catch (IOException ignored) {}
        for (int i = 0; i < x; i++) createTextFile(root.toString(), tcNumber + i);
        return folderName;
    }

    public static String createAFolderWithXNumberOfPNGFiles(String folderPath, int x, String tcNumber) {
        String folderName = tcNumber + "_" + now();
        Path root = Path.of(folderPath, folderName);
        try { Files.createDirectories(root); } catch (IOException ignored) {}
        for (int i = 0; i < x; i++) createPNGFile(5, 5, root.toString(), tcNumber + i);
        return folderName;
    }

    public static FolderMatrix createFolderWithXSubLevelFolders(String folderPath, int numberOfFilesInMainFolder, int numberOfSubFolders, int numberOfFilesInSubFolders, String tcNumber) {
        String folderName = tcNumber + "_" + now();
        Path root = Path.of(folderPath, folderName);
        try { Files.createDirectories(root); } catch (IOException ignored) {}
        String[] mainFiles = new String[numberOfFilesInMainFolder];
        for (int i = 0; i < numberOfFilesInMainFolder; i++) mainFiles[i] = createTextFile(root.toString(), tcNumber + i);
        String[] subFolders = new String[numberOfSubFolders];
        String[][] subFiles = new String[numberOfSubFolders][];
        for (int i = 0; i < numberOfSubFolders; i++) {
            String sub = folderName + "_" + i;
            subFolders[i] = sub;
            Path subPath = root.resolve(sub);
            try { Files.createDirectories(subPath); } catch (IOException ignored) {}
            subFiles[i] = new String[numberOfFilesInSubFolders];
            for (int j = 0; j < numberOfFilesInSubFolders; j++) subFiles[i][j] = createTextFile(subPath.toString(), tcNumber + j);
        }
        return new FolderMatrix(folderName, mainFiles, subFolders, subFiles);
    }

    public record FolderMatrix(String folderName, String[] mainFiles, String[] subFolders, String[][] subFoldersAndFiles) {}

    private static String baseText(String tc) {
        return "TC Number: " + tc + ". This is a text file. " + randAlNum(10) + randNum(5) + now();
    }

    private static String now() {
        return LocalDateTime.now().format(TS);
    }

    private static void writeString(Path p, String s) {
        try {
            Files.createDirectories(p.getParent());
            Files.write(p, s.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {}
    }

    private static void writeImage(String folderPath, String fileName, int w, int h, String fmt) {
        int type = (fmt.equalsIgnoreCase("jpg") || fmt.equalsIgnoreCase("jpeg"))
            ? BufferedImage.TYPE_INT_RGB
            : BufferedImage.TYPE_INT_ARGB;

        BufferedImage img = new BufferedImage(w, h, type);
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                Color c = new Color(R.nextInt(256), R.nextInt(256), R.nextInt(256));
                img.setRGB(x, y, c.getRGB());
            }
        }
        try {
            Files.createDirectories(Path.of(folderPath));
            ImageIO.write(img, fmt, Path.of(folderPath, fileName).toFile());
        } catch (IOException ignored) {}
    }

    private static String randAlNum(int len) {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) sb.append(chars.charAt(R.nextInt(chars.length())));
        return sb.toString();
    }

    private static String randNum(int len) {
        String chars = "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) sb.append(chars.charAt(R.nextInt(chars.length())));
        return sb.toString();
    }

    private static String lowerAlpha(int len) {
        String chars = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) sb.append(chars.charAt(R.nextInt(chars.length())));
        return sb.toString();
    }

    private static byte[] randBytes(int len) {
        byte[] b = new byte[len];
        R.nextBytes(b);
        return b;
    }
}