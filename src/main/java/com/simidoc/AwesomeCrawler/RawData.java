package com.simidoc.AwesomeCrawler;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;

@Data
public class RawData implements Serializable {
    static final long serialVersionUID = 1;
    private String title;
    private String url;
    private String type;
    private byte[] data;
    private String id;

    public RawData(String id) {
        this.id = id;
    }

    public void setUrl(String url){
        this.url = url;
    }
    public String getUrl(){
        return this.url;
    }

    public void setType(String type){
        this.type = type;
    }
    public void setTitle(String title){
        this.title = title;
    }
    public void setData(byte[] data){
        this.data = data;
    }
}


