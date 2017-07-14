package cl.devweb.sample.lucene.sample1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;


public class LuceneTest8 {

	    public static void main(String[] args) throws IOException, ParseException {

	        Analyzer analyzer = new StandardAnalyzer();  // Version.LUCENE_4_10_2
	        Directory directory = new RAMDirectory();
	        IndexWriterConfig config = new IndexWriterConfig( analyzer); // Version.LUCENE_4_10_2,
	        IndexWriter indexWriter = new IndexWriter(directory, config);

	        String[] texts = { "Lucene is an Information Retrieval library written in Java",
	        					"no luzene",
	        					"si Lucene",
	        					"hola mundo" };

	        for(int i=0; i<texts.length; i++) {
	        	Document doc = new Document();
	        	doc.add(new TextField("Content", texts[i], Field.Store.YES));
		        indexWriter.addDocument(doc);
	        }

	        indexWriter.close();

	        // IndexSearcher indexReader = DirectoryReader.open(directory);
	        // IndexSearcher indexSearcher = new IndexSearcher(indexReader);

	        DirectoryReader directoryReader = DirectoryReader.open(directory);
	        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);


	        QueryParser parser = new QueryParser("Content", analyzer);
	        Query query = parser.parse("lucene"); // busqueda - case insensitive - palabra completa

	        int hitsPerPage = 10;

	        //paginacion: hitsPerPage
	        TopDocs docs = indexSearcher.search(query, hitsPerPage);
	        ScoreDoc[] hits = docs.scoreDocs;

	        int end = Math.min(hits.length, hitsPerPage);

	        System.out.println("Total Hits: " + hits.length);
	        System.out.println("Results: ");

	        for (int i = 0; i < end; i++) {
	            Document d = indexSearcher.doc(hits[i].doc);
	            System.out.println( hits[i].doc + " Content: " + d.get("Content") );
	        }


	    }

	    //paginacion:
	    /*
	    public List<Document> getPage(int from , int size){
	        List<Document> documents = new ArrayList<Document>();
	        Query query = parser.parse(searchTerm);
	        TopDocs hits = searcher.search(query, maxNumberOfResults);

	        int end = Math.min(hits.totalHits, size);

	        for (int i = from; i < end; i++) {
	            int docId = hits.scoreDocs[i].doc;

	            //load the document
	            Document doc = searcher.doc(docId);
	            documents.add(doc);
	        }

	        return documents;
	    }*/


	}


