package com.simidoc.AwesomeCrawler;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.*;

import java.util.Scanner;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


public class Utils {
    final String authUser = "lum-customer-onesiness-zone-static10";
    final String authPassword = "kz53twdp74rd";

    public String getPDFurl(String page) throws IOException {
        Authenticator.setDefault(
                new Authenticator() {
                    @Override
                    public PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(authUser, authPassword.toCharArray());
                    }
                }
        );
        System.setProperty("http.proxyUser", authUser);
        System.setProperty("http.proxyPassword", authPassword);
        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
        Proxy proxy = new Proxy(
                Proxy.Type.HTTP,
                InetSocketAddress.createUnresolved("zproxy.lum-superproxy.io", 22225)
        );
        Connection conn = Jsoup.connect(page)
                                .proxy(proxy)
                                .userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0") //
                                .header("Content-Language", "en-US");
        Document doc = conn.get();
        Element pdfButton = doc.select("a.button--primary").first();
        Element alternateSource = doc.select("a.alternate-source-link-button").first();
        Pattern p = Pattern.compile("\\.pdf$");
        String PDFurl = null;
        if (pdfButton != null && p.matcher(pdfButton.attr("href")).find()){
            PDFurl = pdfButton.attr("href");
        }
        else if (alternateSource != null && p.matcher(alternateSource.attr("link")).find()){
            PDFurl = alternateSource.attr("link");
        }
        return PDFurl;
    }
    public String getNumberfromPath(String path){
        Pattern pattern = Pattern.compile("\\d+", Pattern.CASE_INSENSITIVE);
        Matcher m = pattern.matcher(path);
        String id = null;
        if (m.find()) {
            id = m.group(0);
        }
        return id;
    }

    public BufferedReader readJsonlGzip(String path) throws IOException {
        InputStream fileStream = new FileInputStream(path);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
        BufferedReader buffered = new BufferedReader(decoder);

        return buffered;
    }

    public List<String> readBatch(BufferedReader reader, int batchSize) throws IOException {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            String line = reader.readLine();
            if (line != null) {
                result.add(line);
            } else {
                return result;
            }
        }
        return result;
    }

    public JSONObject createMetaObject(List<String> metaLines) throws IOException {
        JSONObject objDict = new JSONObject();
        for(String line : metaLines) {
            JSONObject obj = new JSONObject(line);
            try{
                JSONObject newobj = new JSONObject();
                String paper_id = obj.getString("paper_id");
                newobj.put("paper_id", paper_id);
                newobj.put("title", obj.getString("title"));
                if (!obj.isNull("mag_field_of_study")){
                    newobj.put("field_of_study", obj.getJSONArray("mag_field_of_study").getString(0));
                }
                else{
                    newobj.put("field_of_study", "None");
                }
                newobj.put("s2_url", obj.getString("s2_url"));
                objDict.put(paper_id, newobj);
            }
            catch (Exception e){
                System.out.println(obj);
                e.printStackTrace();
            }
        }
        return objDict;
    }

    public JSONArray readPDFparserFile(String filename) throws IOException {
        InputStream fileStream = new FileInputStream(filename);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
        BufferedReader buffered = new BufferedReader(decoder);
        String thisLine = null;
        JSONArray objArray = new JSONArray();
        while ((thisLine = buffered.readLine()) != null) {
            JSONObject obj = new JSONObject(thisLine);
            JSONArray abstractObj = obj.getJSONArray("abstract");
            String abstractText = null;
            if (abstractObj.length() > 0){
                abstractText = abstractObj.getJSONObject(0).getString("text");
            }
            JSONArray bodyObj = obj.getJSONArray("body_text");
            String bodyText = null;
            if (bodyObj.length() > 0){
                for(int i=0; i<bodyObj.length(); i++){
                    String text = bodyObj.getJSONObject(i).getString("text");
                    if (text != null){
                        bodyText = bodyText + System.lineSeparator() + text;
                    }
                }
            }
            if (bodyText != null &&  abstractText != null){
                JSONObject newobj = new JSONObject();
                newobj.put("paper_id", obj.get("paper_id"));
                newobj.put("data", abstractText + System.lineSeparator() + bodyText);

                objArray.put(newobj);
            }
            return objArray;
        }
        return objArray;
    }

    public void writeJSONfile(String path, JSONObject data){
        try {
            File file = new File(path);
            file.getParentFile().mkdirs();
            FileWriter writer = new FileWriter(file);
            writer.write(data.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
