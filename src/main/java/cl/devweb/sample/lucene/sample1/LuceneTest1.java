package cl.devweb.sample.lucene.sample1;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.Field;


public class LuceneTest1 {

    public static void main(String[] args) throws IOException {

        Analyzer analyzer = new WhitespaceAnalyzer();  //  Version.LUCENE_4_10_2

        Directory directory = new RAMDirectory(); // FSDirectory on a file system.
        IndexWriterConfig config = new IndexWriterConfig(analyzer); // Version.LUCENE_4_10_2
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document doc = new Document();

        String text = "Lucene is an Information Retrieval library written in Java";

        doc.add(new TextField("fieldname", text, Field.Store.YES));

        indexWriter.addDocument(doc);

        directory.close();
        indexWriter.close();

        System.out.println("stop.");
    }

}


