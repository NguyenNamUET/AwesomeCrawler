package com.simidoc.AwesomeCrawler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.rocksdb.*;
import org.xerial.snappy.Snappy;


import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.nio.charset.StandardCharsets;
import org.json.*;


public class Main {
    public static RocksDB init(String file) throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options()
                .setCreateIfMissing(true)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCompactionPriority(CompactionPriority.ByCompensatedSize)
                .setCompressionType(CompressionType.NO_COMPRESSION);
        return RocksDB.open(options, file);
    }
    static ObjectMapper mapper = new ObjectMapper();
    public static void index(RocksDB rocksDB, String id, String url, String type, String title, String data){
        RawData rawData = new RawData(id);
        rawData.setUrl(url);
        rawData.setType(type);
        rawData.setTitle(title);
        rawData.setData(data.getBytes());
        try {
            byte[] key = DigestUtils.sha256(rawData.getUrl());
            byte[] value = Snappy.compress(mapper.writeValueAsString(rawData).getBytes());
            rocksDB.put(key, value);

        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws RocksDBException, IOException {
        RocksDB rocksDB = init("rock.db");
        Utils utils = new Utils();

        String METADATAPATH = "/home/nguyennam/Downloads/s2orc_full/20200705v1/full/metadata/";
        String PDFPATH = "/home/nguyennam/Downloads/s2orc_full/20200705v1/full/pdf_parses/";
        File metadataPath = new File(METADATAPATH);
        File[] metadataGZfiles = metadataPath.listFiles();

        if(metadataGZfiles == null){
            System.out.println("Not directory");
        }
        else {
            for (File file : metadataGZfiles) {
                try {
                    BufferedReader metaReader = utils.readJsonlGzip(file.getAbsolutePath());
                    int batchSize = 1000;
                    boolean moreLines = true;
                    while (moreLines) {
                        List<String> linesBatch = utils.readBatch(metaReader, batchSize);
                        System.out.println(linesBatch.size());
                        /*** DO INDEXING ***/
                        JSONObject metadataDict = utils.createMetaObject(linesBatch);

                        String id = utils.getNumberfromPath(file.getName());
                        BufferedReader pdfReader = utils.readJsonlGzip(PDFPATH + "pdf_parses_" + id + ".jsonl.gz");
                        String thisLine = null;
                        while ((thisLine = pdfReader.readLine()) != null) {
                            JSONObject pdfObj = new JSONObject(thisLine);
                            String paper_id = pdfObj.getString("paper_id");
                            if (metadataDict.has(paper_id)) {
                                JSONArray abstractObj = pdfObj.getJSONArray("abstract");
                                String abstractText = "";
                                if (abstractObj.length() > 0){
                                    abstractText = abstractObj.getJSONObject(0).getString("text");
                                }
                                JSONArray bodyObj = pdfObj.getJSONArray("body_text");
                                String bodyText = "";
                                if (bodyObj.length() > 0){
                                    for(int i=0; i<bodyObj.length(); i++){
                                        String text = bodyObj.getJSONObject(i).getString("text");
                                        if (text != null){
                                            bodyText = bodyText + System.lineSeparator() + text;
                                        }
                                    }
                                }
                                JSONObject metaObj = metadataDict.getJSONObject(paper_id);
                                try{
                                    JSONObject docObj = new JSONObject();
                                    // Insert paper id
                                    docObj.put("paper_id", paper_id);
                                    // Insert content data
                                    docObj.put("data", abstractText + System.lineSeparator() + bodyText);
                                    // Insert title
                                    docObj.put("title", metaObj.getString("title"));
                                    // Insert type
                                    String type = metadataDict.getJSONObject(paper_id).getString("field_of_study");
                                    docObj.put("type", type);
                                    // Insert URL
                                    String docUrl = utils.getPDFurl(metaObj.getString("s2_url"));
                                    docObj.put("url", Objects.requireNonNullElse(docUrl, ""));
                                index(rocksDB,  paper_id,
                                                docObj.getString("url"),
                                                type,
                                                docObj.getString("title"),
                                                docObj.getString("data"));
                                    utils.writeJSONfile("/home/nguyennam/Downloads/s2data/"+type+"/paper_"+paper_id+".jsonl",
                                            docObj);
                                    System.out.println("Success");
                                }
                                catch (Exception e){
                                    System.out.println("Paper "+paper_id+" caused error: "+e.toString());
                                    utils.writeJSONfile("/home/nguyennam/Downloads/s2data/error/paper_"+paper_id+".jsonl",
                                            metaObj);
                                }
                            }
                        }
                        if (linesBatch.size() < batchSize) {
                            moreLines = false;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
