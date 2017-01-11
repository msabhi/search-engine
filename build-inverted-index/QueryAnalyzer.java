/**************************************************************************************
*
*
*	BUILD INVERTED INDEX ON CORPUS OF DOCUMENTS AND RANK THE DOCUMENTS BASED ON 
*   5 RETRIEVAL MODELS - Okapi TF, Okapi IDF, Okapi BM25, LM Laplace, JM Smoothing
*
*
**************************************************************************************/



package com.neu.ir.invertedidx;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.neu.ir.utils.*;

import java.io.*;


import org.springframework.core.NestedRuntimeException;
import org.springframework.web.client.RestTemplate;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.json.JSONObject;
import org.json.JSONException;

public class QueryAnalyzer {

	public static final String ES_SERVICE_URL = "http://localhost:9200/ap_dataset/document/";

	public static final String searchUrl = SERVICE_URL + "_search?q=";

	public static final String TERM_ID_MAP_OBJ_SER_FILE = "resources/termidmap.ser";

	public static final String ID_DOC_LEN_MAP_OBJ__SER_FILE = "resources/iddoclenmap.ser";
	public static final String DOC_FREQ_SER = "resources/docfreq.ser";

	final static String termvectorOptions = "/_termvector?"
			+ "fields=text,docno&doc_freq=true&offsets=true&term_statistics=true"
			+ "&field_statistics=true&payloads=true&positions=true";

	private double avgDocumentLength;

	private Map<String, ArrayList<HashMap<Integer, Double>>> termIdMap = new HashMap<String, ArrayList<HashMap<Integer, Double>>>();

	private Map<String, Double> termDocFreqMap = new HashMap<String, Double>();

	private Map<Integer, Double> idDocLenMap = new HashMap<Integer, Double>();

	private Map<Integer, String[]> queryWordsMap = new HashMap<Integer, String[]>();

	private Set<String> allQueryTermsSet = new HashSet<String>();

	private Set<String> stopwordsSet = new HashSet<String>();

	private Map<Integer, String> idDocNoMap= new HashMap<Integer, String>();

	private Map<String, Double> okapiValue;

	private Map<String, Double> ttfValueMap = new HashMap<String, Double>();

	private Map<String, Double> bm25ValueMap =  new HashMap<String, Double>();

	private Map<String, Double> lmLaplaceMap = new HashMap<String, Double>();

	private Map<String, Double> jmSmoothingMap = new HashMap<String, Double>();

	private Map<String, Double> totalTermFreqMap = new HashMap<String, Double>();

	private Set<Integer> allQueryIdsSet = new HashSet<Integer>();

	public final static Double totalNoOfDocs = 84678.0;  // calculated from DocumentBuilder program

	public static double vocabularySize;

	public static double totalLengthOfAllDocs = 0;

	public static void main(String[] args) throws NestedRuntimeException, JSONException, IOException, ClassNotFoundException{
		QueryAnalyzer qa = new QueryAnalyzer();
		qa.initiailize();
		File termIdMapSer = new File(TERM_ID_MAP_OBJ_SER_FILE);
		File idDocLenMapSer = new File(ID_DOC_LEN_MAP_OBJ_SER_FILE);

		if(termIdMapSer.exists() && idDocLenMapSer.exists())
		{
			FileInputStream tidf = new FileInputStream(TERM_ID_MAP_OBJ_SER_FILE);
			ObjectInputStream otid = new ObjectInputStream(tidf);
			qa.termIdMap = (HashMap) otid.readObject();
			otid.close();
			tidf.close();

			FileInputStream idf = new FileInputStream(ID_DOC_LEN_MAP_OBJ_SER_FILE);
			ObjectInputStream oid = new ObjectInputStream(idf);
			qa.idDocLenMap = (HashMap) oid.readObject();
			idf.close();
			otid.close();

			FileInputStream ddf = new FileInputStream(DOC_FREQ_SER);
			ObjectInputStream odf = new ObjectInputStream(ddf);
			qa.termDocFreqMap = (HashMap) odf.readObject();
			odf.close();
			ddf.close();
		}
		else{
			qa.buildInvertedIndex();
			FileOutputStream tidf = new FileOutputStream(TERM_ID_MAP_OBJ_SER_FILE);
			ObjectOutputStream otid = new ObjectOutputStream(tidf);
			otid.writeObject(qa.termIdMap);
			tidf.close();
			otid.close();

			FileOutputStream idf = new FileOutputStream(ID_DOC_LEN_MAP_OBJ_SER_FILE);
			ObjectOutputStream oid = new ObjectOutputStream(idf);
			oid.writeObject(qa.idDocLenMap);
			idf.close();
			oid.close();


			FileOutputStream ddf = new FileOutputStream(DOC_FREQ_SER);
			ObjectOutputStream odf = new ObjectOutputStream(ddf);
			odf.writeObject(qa.termDocFreqMap);
			odf.close();
			ddf.close();
		}

		System.out.println("Printing Terms and ids "+qa.termIdMap.size() + "Query terms size" + qa.allQueryTermsSet.size());
		Iterable id =  qa.idDocLenMap.values();
		for (int i =1;i<=totalNoOfDocs;i++) {
			double Length = qa.idDocLenMap.get(i);
			totalLengthOfAllDocs =  totalLengthOfAllDocs + Length;
		}
		qa.avgDocumentLength = totalLengthOfAllDocs/totalNoOfDocs;
		System.out.println("Average document length:" + qa.avgDocumentLength);
		qa.calculateOkapi_tf();
	}

	/*
		Initialize the program with stop words, query map, id-doc no map
		and vocabulary size
	 */
	private void initiailize() throws IOException
	{
		Config config = Config.getInstance("resources/hw1-config.txt");
		String queryFile =config.getQueryFilePath();
		String stopwordFile = config.getStopListPath();
		String idDocNoFile = config.getIdDocNoPath();
		initializeStopWordsSet();
		initializeQueryMap(queryFile, stopwordFile);
		initializeIdDocNoMap(idDocNoFile);
		vocabularySize = getVocabularySize("ap_dataset", "document", "text");
	}

	private void initializeIdDocNoMap(String idDocNoFile) throws NumberFormatException, IOException
	{
		BufferedReader ibr = new BufferedReader(new FileReader(idDocNoFile));
		String line = ibr.readLine();
		while((line=ibr.readLine())!=null)
		{
			String[] idDocno = line.split(" ");
			Integer id = Integer.parseInt(idDocno[0]);
			idDocNoMap.put(id, idDocno[1]);
		}
		System.out.println(idDocNoMap.size());
	}

	/*
		
	*/
	public static double getVocabularySize(String index, String type, String field) {
		Node node = nodeBuilder().client(true).node();
		Client client = node.client();
		MetricsAggregationBuilder aggregation = AggregationBuilders
				.cardinality("agg").field(field);
		SearchResponse sr = client.prepareSearch(index).setTypes(type)
				.addAggregation(aggregation).execute().actionGet();

		Cardinality agg = sr.getAggregations().get("agg");
		double value = agg.getValue();
		client.close();
		node.close();
		return value;	
	}

	/*
		Given the query file with records like below : #id query_line, the hash map with key as
		id and various words in the query line is split into array as hash map value
		Also, normalizes the query words the same way the document words were normalized while indexing
	 */
	private void initializeQueryMap(String queryFile, String stopWordFile) throws IOException
	{
		BufferedReader sbr = new BufferedReader(new FileReader(stopWordFile));
		String line, formattedLine;
		while((line=sbr.readLine())!=null){
			line=line.trim();
			stopwordsSet.add(line);
		}
		sbr.close();
		BufferedReader qbr = new BufferedReader(new FileReader(queryFile));
		int id = 1;
		line=qbr.readLine();
		while(line!=null && ! line.equals("")){
			String tempLine = line;		
			line=qbr.readLine();
			if (line==null) break;
			if(line.startsWith("[a-zA-Z]")){
				tempLine += line;
				line = qbr.readLine();
			}
			id = Integer.parseInt(tempLine.charAt(0) + "" +tempLine.charAt(1));
			formattedLine = tempLine.replaceAll(".*Document (will|must) (include|report|describe|discuss|identify|cite|predict)", "");
			queryWordsMap.put(id, getStemmedWords(formattedLine).split(" "));	// reduce the query words to stem word 
		}		
		qbr.close();
	}

	/*
		Logic to normalize the query/document words by applying 
		1> Stem the words
		2> Fix the known issues in a few words
	*/
	private String getStemmedWords(String q)
	{
		PorterStemmer stemmer = new PorterStemmer();
		String[] stemmedWords = q.replace("-"," ").replaceAll("[^a-zA-Z ]", "").split(" ");
		String newQuery = "";
		for (String word : stemmedWords){
			if(! stopwordsSet.contains(word)){
				String stemmedWord = stemmer.stem(word.toLowerCase());
				allQueryTermsSet.add(stemmedWord.toLowerCase());
				newQuery += " " + stemmedWord.toLowerCase();
			}
		}
		newQuery = newQuery.replaceAll(" us ", " u.s. ").replaceAll("detat", "d'etat").replaceFirst(" ", "");
		return newQuery;
	}

	/*
		Given a query word, determines the total frequencies of the term across the corpus
		stored in our inverted index data structure
	*/
	private void calTotalTermFreqOfEveryQryTerm(){

		Set<Integer> keys = queryWordsMap.keySet();
		Iterator<Integer> qkeyIt = keys.iterator();
		while(qkeyIt.hasNext()){
			int docId = qkeyIt.next();
			String[] query = queryWordsMap.get(docId);
			for (String term : query)
			{
				// if the query term is not present in our index, lets ignore it
				if(!termIdMap.containsKey(term) || totalTermFreqMap.containsKey(term)) continue; 

				Double totalFreq = 0.0;
				for(Map ids : termIdMap.get(term))
				{
					HashMap.Entry<Integer,Double> entry= (Entry<Integer, Double>) ids.entrySet().iterator().next();
					int id = entry.getKey();

					Double freq = entry.getValue();
					totalFreq += freq; 
				}
				System.out.println(totalFreq);
				totalTermFreqMap.put(term, totalFreq);
			}
		}

	}

	/*
		Given the normalized query words and inverted indexed words data structure,
		determine the ranks of all the documents for a query by apply 5 different retrieval algorithms -
		Okapi TF, Okapi IDF, Okapi BM25, LM Laplace, JM Smoothing
	*/
	private void calculateRetrievalModels() throws IOException
	{
		BufferedWriter okapibw = new BufferedWriter(new FileWriter("resources/okapi.txt"));
		BufferedWriter ttfbw = new BufferedWriter(new FileWriter("resources/ttf.txt"));
		BufferedWriter bm25bw = new BufferedWriter(new FileWriter("resources/bm25.txt"));
		BufferedWriter lmbw = new BufferedWriter(new FileWriter("resources/lmlaplace.txt"));
		BufferedWriter jmbw = new BufferedWriter(new FileWriter("resources/jmsmoothing.txt"));


		double k1 = 1.2;
		double k2 = 500;
		double b = 0.75;

		okapiValue = new HashMap<String, Double>();
		int count = 1;
		Set<Integer> keys = queryWordsMap.keySet();
		Iterator<Integer> qkeyIt = keys.iterator();
		calTotalTermFreqOfEveryQryTerm()
		qkeyIt = keys.iterator();

		while(qkeyIt.hasNext()){
			int docId = qkeyIt.next();
			Map<String, HashSet<Integer>> termIdMapSet = new HashMap<String, HashSet<Integer>>();
			Set<Integer> allQueryIdsSet = new HashSet<Integer>();
			count++;
			String[] query = queryWordsMap.get(docId);
			System.out.println();

			// Calculate Okapi TF, Okapi IDF, Okapi BM25, LM Laplace, JM Smoothing
			for (String term : query)
			{
				double termQueryFreq = findTermQueryFreq(term, query);
				if(termIdMap.containsKey(term)){					
					Double docFreq = termDocFreqMap.get(term);
					for(Map ids : termIdMap.get(term))
					{			

						HashMap.Entry<Integer,Double> entry= (Entry<Integer, Double>) ids.entrySet().iterator().next();
						int id = entry.getKey();

						Double freq = entry.getValue();
						double dl = idDocLenMap.get(id);
						double lambda = 0.5; // dl / (dl + avgDocumentLength);

						Double okapi_value =  Math.round((freq /(freq + 0.5 + 1.5 * (idDocLenMap.get(id) / avgDocumentLength))) * 100.0 ) / 100.0;
						if(termIdMapSet.containsKey(term)){
							HashSet<Integer> idSet = termIdMapSet.get(term);
							idSet.add(id);
							termIdMapSet.put(term, idSet);
						}
						else{
							HashSet<Integer> idSet = new HashSet<Integer>();
							idSet.add(id);
							termIdMapSet.put(term, idSet);
						}
						allQueryIdsSet.add(id);



						if(okapiValue.containsKey(idDocNoMap.get(id))){
							//Set Okapi Value
							Double value = okapiValue.get(idDocNoMap.get(id));
							Double newValue = value + okapi_value;
							okapiValue.put(idDocNoMap.get(id), newValue);

							//Set TTF Value
							Double ttfValue = (double) Math.round((Math.log10(totalNoOfDocs)/Math.log10(docFreq)) * 100.0 / 100.0);
							ttfValueMap.put(idDocNoMap.get(id), Double.sum(value , (okapi_value * ttfValue)));

							//Set BM25 Value
							double bm25Value = Math.log10((totalNoOfDocs+0.5)/(docFreq + 0.5)) * ((freq * (1 + k1))/(freq + k1 * ((1-b)+ b * (idDocLenMap.get(id)/avgDocumentLength))) * (termQueryFreq * (1+k2)/(termQueryFreq+k2)));
							double newBm25Value = bm25ValueMap.get(idDocNoMap.get(id));
							bm25ValueMap.put(idDocNoMap.get(id), newBm25Value + bm25Value);

							//Set LM Laplace Smoothing Value
							Double lmLaplaceValue = lmLaplaceMap.get(idDocNoMap.get(id));
							//double newlmLaplaceValue = Math.log10(freq+1)-Math.log10(vocabularySize+idDocLenMap.get(id));
							Double newlmLaplaceValue = Math.log10((freq+1)/(vocabularySize+idDocLenMap.get(id)));
							lmLaplaceMap.put(idDocNoMap.get(id), lmLaplaceValue + newlmLaplaceValue);

							double jmSmoothingValue = jmSmoothingMap.get(idDocNoMap.get(id));
							double newJmSmoothingValue = lambda * (freq/idDocLenMap.get(id)) + ((1 - lambda) * ((totalTermFreqMap.get(term)-freq)/(totalLengthOfAllDocs-idDocLenMap.get(id))));;
							jmSmoothingMap.put(idDocNoMap.get(id), jmSmoothingValue + Math.log10(newJmSmoothingValue));
						}
						else {
							//Set Okapi Value 
							okapiValue.put(idDocNoMap.get(id), okapi_value);

							//Set TTF Value
							Double ttfValue = (double) Math.round((Math.log10(totalNoOfDocs)/Math.log10(docFreq)) * 100.0 / 100.0);
							ttfValueMap.put(idDocNoMap.get(id), okapi_value * ttfValue);

							//Set BM25 Value
							double bm25Value = Math.log10((totalNoOfDocs+0.5)/(docFreq + 0.5)) * ((freq * (1 + k1))/(freq + k1 * ((1-b) + b * (idDocLenMap.get(id)/avgDocumentLength))) * (termQueryFreq * (1+k2)/(termQueryFreq+k2)));
							bm25ValueMap.put(idDocNoMap.get(id), bm25Value);

							//Set lmlaplace value
							Double newlmLaplaceValue = Math.log10((freq+1)/(vocabularySize+idDocLenMap.get(id)));
							lmLaplaceMap.put(idDocNoMap.get(id), (newlmLaplaceValue));

							//Set JmSmoothing value
							System.out.println(totalTermFreqMap.get(term)+ " "+(totalTermFreqMap.get(term)-freq)/(totalLengthOfAllDocs-idDocLenMap.get(id)));
							double newJmSmoothingValue = lambda * (freq/idDocLenMap.get(id)) + ((1 - lambda) * ((totalTermFreqMap.get(term)-freq)/(totalLengthOfAllDocs-idDocLenMap.get(id))));
							jmSmoothingMap.put(idDocNoMap.get(id), Math.log10(newJmSmoothingValue));
						}

					}
				}else{
					System.out.println("NOT THERE"+ term);
				}
			}
			String[] terms = queryWordsMap.get(docId);

			for (int i =0 ; i < terms.length; i++)
			{
				if(! termIdMapSet.containsKey(terms[i])) continue;
				System.out.println("TERM "+ terms[i]);
				Set<Integer> tempSet = new HashSet<Integer>();
				tempSet.addAll(allQueryIdsSet);
				tempSet.removeAll(termIdMapSet.get(terms[i]));
				System.out.println("QUERY ID SET " + allQueryIdsSet.size()+ " ORIGINAL "+termIdMapSet.get(terms[i]).size()+ " AFTER REMOVAL " + tempSet.size());
				for (Integer id : tempSet)
				{
					if(lmLaplaceMap.containsKey(idDocNoMap.get(id))){
						double newlmLaplaceValue = Math.log10(0+1/(vocabularySize+idDocLenMap.get(id)));
						Double lmLaplaceValue = lmLaplaceMap.get(idDocNoMap.get(id));
						lmLaplaceMap.put(idDocNoMap.get(id), (lmLaplaceValue + newlmLaplaceValue));

						Double jmSmoothingValue = jmSmoothingMap.get(idDocNoMap.get(id));
						Double newJmSmoothingValue = ((1 - 0.5) * ((totalTermFreqMap.get( terms[i]))/(totalLengthOfAllDocs-idDocLenMap.get(id))));
						jmSmoothingMap.put(idDocNoMap.get(id), jmSmoothingValue + Math.log10(newJmSmoothingValue));
					}
					else
					{
						double newlmLaplaceValue = Math.log10(0+1/(vocabularySize+idDocLenMap.get(id)));
						lmLaplaceMap.put(idDocNoMap.get(id), newlmLaplaceValue);

						Double newJmSmoothingValue = ((1 - 0.5) * ((totalTermFreqMap.get( terms[i]))/(totalLengthOfAllDocs-idDocLenMap.get(id))));
						jmSmoothingMap.put(idDocNoMap.get(id), newJmSmoothingValue);
					}
				}
			}

			// Sort the documents for every algorithm depending on the individual 
			// document score
			okapiValue = sortByComparator(okapiValue, false);
			ttfValueMap = sortByComparator(ttfValueMap, false); 
			bm25ValueMap = sortByComparator(bm25ValueMap, false);
			lmLaplaceMap = sortByComparator(lmLaplaceMap, false);
			jmSmoothingMap = sortByComparator(jmSmoothingMap, false);

			//fetch first 100 and write to a file
			writeFirst100toFile(okapiValue, docId, okapibw);
			writeFirst100toFile(ttfValueMap, docId, ttfbw);
			writeFirst100toFile(bm25ValueMap, docId, bm25bw);
			writeFirst100toFile(lmLaplaceMap, docId, lmbw);
			writeFirst100toFile(jmSmoothingMap, docId, jmbw);

			okapiValue = new HashMap<String, Double>();
			ttfValueMap = new HashMap<String, Double>();
			bm25ValueMap = new HashMap<String, Double>();
			lmLaplaceMap = new HashMap<String, Double>();
			jmSmoothingMap = new HashMap<String, Double>();
		}
		okapibw.close();
		ttfbw.close();
		bm25bw.close();
		lmbw.close();
		jmbw.close();
	}

	private double findTermQueryFreq(String term, String[] query)
	{
		double count = 0;
		for (String everyTerm : query)
		{
			if (everyTerm.equals(term)) count++;
		}

		return count;
	}

	// A generic function to sort an hashmap depending on the count of value 
	private  Map<String, Double> sortByComparator(Map<String, Double> unsortMap, final boolean order)
	{
		List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Double>>()
				{
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2)
			{
				if (order)
				{
					return o1.getValue().compareTo(o2.getValue());
				}
				else
				{
					return o2.getValue().compareTo(o1.getValue());

				}
			}
				});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Entry<String, Double> entry : list)
		{
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	//Write the top 100 ranked best documents to the file
	private void writeFirst100toFile(Map<String, Double> scoresMap, int queryId, BufferedWriter bw) throws IOException
	{
		int count=1;
		Iterator entryIterator = scoresMap.entrySet().iterator();
		while(entryIterator.hasNext()){
			Entry<String, Double> entry = (Entry<String, Double>) entryIterator.next();
			bw.write(queryId+" Q0 "+entry.getKey()+ " " + count + " " + entry.getValue() + " Exp\n");
			count++;
		}
	}


	/*
    	builds the inverted index mapping the term to a list of all [< Doc id it appears in ,
		number of times it appears in the Doc id>...]
	 */
	private void buildInvertedIndex() throws JSONException
	{
		RestTemplate restTemplate = new RestTemplate();
		int corpuslength = 0;
		for(int id=1;id<=totalNoOfDocs;id++)
		{
			String termVectorUrl = ES_SERVICE_URL + id + termvectorOptions;
			String termVectorResult = restTemplate.getForObject(termVectorUrl, String.class);
			double docLength = getDocumentLength(termVectorResult,id);
			idDocLenMap.put(id, docLength);
		}
	}

	/*
		getDocumentLength parses the JSON object obtained by querying the Id and calculates the 
		document length == (no. of all the terms in the document)

	 */
	private int getDocumentLength(String termVectorJSONResult, int id) throws JSONException
	{
		int count=0;
		int length=0;
		JSONObject jb = new JSONObject(termVectorJSONResult);   
		JSONObject allterms;
		try{
			allterms =  jb.getJSONObject("term_vectors").
					getJSONObject("text").getJSONObject("terms");
			Iterator allTermsIter = allterms.keys();
			while(allTermsIter.hasNext())
			{
				String key = allTermsIter.next().toString();
				JSONObject term = allterms.getJSONObject(key);
				if(allQueryTermsSet.contains(key)){
					ArrayList<HashMap<Integer,Double>> temp = new ArrayList<HashMap<Integer,Double>>();
					if(termIdMap.containsKey(key)){
						temp = termIdMap.get(key);
					}		
					termDocFreqMap.put(key, term.getDouble("doc_freq"));
					HashMap<Integer, Double> tempMap = new HashMap<Integer, Double>();
					tempMap.put(id,(double) term.getInt("term_freq"));
					temp.add(tempMap);
					termIdMap.put(key, temp);
				}
				length+=term.getInt("term_freq");
				term=null;
			}
			allTermsIter=null;
		}
		catch(JSONException ex) {
			System.out.println("FAILED AT ID "+ id);
			count++;
		}		
		jb=null;
		return length;
	}


		/*
		Add all the stop words to a set
		TODO : move these initializations to a file
	 */
	private void initializeStopWordsSet()
	{
		stopwordsSet.add("role");
		stopwordsSet.add("efforts");
		stopwordsSet.add("aid");
		stopwordsSet.add("making");
		stopwordsSet.add("anticipate");
		stopwordsSet.add("report");
		stopwordsSet.add("ongoing");
		stopwordsSet.add("development");
		stopwordsSet.add("identify");
		stopwordsSet.add("event");
		stopwordsSet.add("result");
		stopwordsSet.add("type");
		stopwordsSet.add("location");
		stopwordsSet.add("determine");
		stopwordsSet.add("basis");
		stopwordsSet.add("instances");
		stopwordsSet.add("taken");		
	}

}

