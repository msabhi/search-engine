package com.neu.ir.hw1;
import com.neu.ir.utils.*;

import static org.elasticsearch.node.NodeBuilder.*;

import java.io.*;
import java.io.*;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class IndexToElasticSearch{

	public static final CONFIG_FILE_PATH = "resources/hw1-config.txt";

	// get all the documents in the directory path 
	public static File[] getAllDocuments(){
		Config config = Config.getInstance(CONFIG_FILE_PATH);
		File collectionsDir = new File(config.getCollectionDirPath());
		File[] listOfFiles = collectionsDir.listFiles();
		return listOfFiles;
	}

	// index every document into elastic search data store with fields "docno" and "text"
	// by parsing the DOC elements file by file
	public static void indexFilesIntoElasticSearch() throws IOException, SAXException, ParserConfigurationException{

		Node node = nodeBuilder().client(true).node();
		Client client = node.client();
		int count = 0;
		for(File everyFile : getAllDocuments()){
			if( everyFile.getName().startsWith("ap")) {
				BufferedReader br = new BufferedReader(new FileReader(everyFile));
				String line;
				while(( line=br.readLine()) != null){
					count++;			
					String xmlStr ="";
					while(! line.equals("</DOC>")){
						xmlStr = xmlStr +" "+ line;
						line=br.readLine();
					}
					xmlStr = xmlStr + line;    
					org.jsoup.nodes.Document doc = Jsoup.parse(xmlStr);
					Elements docno = doc.getElementsByTag("DOCNO");
					Elements textelems = doc.getElementsByTag("TEXT");
					String allTextElements = "";
					for(Element text: textelems){
						allTextElements += text.html() + " ";
					}
					
					// insert the doc object into elastic search
					IndexRequestBuilder indexRequestBuilder = client.prepareIndex("ap_dataset", "document",""+count);
					XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
					contentBuilder.startObject();
					contentBuilder.field("docno", docno.html());
					contentBuilder.field("text", allTextElements);
					contentBuilder.endObject();
					indexRequestBuilder.setSource(contentBuilder);
					IndexResponse response = indexRequestBuilder.execute().actionGet();
				}
			}
		}		
		node.close();
	}

	
	public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException{
		indexFilesIntoElasticSearch();
	}

