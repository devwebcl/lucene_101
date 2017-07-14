package cl.devweb.sample.lucene.lucenetutorial.factory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class IndexingDatabasesFactory {

	public static void main(String[] args) throws SQLException, IOException {

		System.out.println("Start.\n"+(new Date())+"\n");
		indexDatabase();
		System.out.println("Fin indexing.\n"+(new Date())+"\n");

		search("torres");
		System.out.println("\n"+(new Date())+"\n");
	}


	private static void indexDatabase() throws IOException, SQLException {

        //indexing directory
        Path path = Paths.get("/Users/German/tmp/lucene/indexes");
        Directory directory = FSDirectory.open(path);

        IndexWriterConfig config = new IndexWriterConfig(new SimpleAnalyzer());
        config.setCodec(new Lucene54Codec(Mode.BEST_COMPRESSION));  // BEST_SPEED  BEST_COMPRESSION

        IndexWriter indexWriter = new IndexWriter(directory, config);
        indexWriter.deleteAll();

		System.out.println("conectando...\n");
		String url = "t3://localhost:7001";

		java.sql.Connection conn = null;
		java.sql.Statement stmt = null;

		try {

			Context ctx = null;
			Hashtable<String, String> ht = new Hashtable<String, String>();
			ht.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
			ht.put(Context.PROVIDER_URL, url);

			ctx = new InitialContext(ht);
			javax.sql.DataSource ds = (javax.sql.DataSource) ctx.lookup("JDBC/VISTA_360_CN");
			conn = ds.getConnection();
			conn.setAutoCommit(true);

			long tamanno = 0;
			stmt = conn.createStatement();

			String sql = "SELECT sc.person_uid, sc.fst_name, sc.last_name, sc.mid_name FROM vista_360_cn.s_contact sc";
			sql = sql + " WHERE last_name = 'Torres'";
			// Loop:
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				Document doc = new Document();
				String s = rs.getString("fst_name") + " " + rs.getString("last_name"); //prototipo de estructura de dato
				//System.out.println("s=" + s );

				if(s != null) {
					tamanno = tamanno + s.length();
					doc.add(new TextField("contents", s, Store.YES));
				}
				indexWriter.addDocument(doc);
			}

			indexWriter.commit(); // optional ?

			// Note that this may be a costly operation, so, try to re-use a single writer instead of closing and opening a new one.
			// See commit() for caveats about write caching done by some IO devices.
			indexWriter.close();
			directory.close();


			System.out.println("tamanno = " + tamanno);
			System.out.println("fin.");

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
			} catch (SQLException e) {
				throw e;
			} finally {
				try {
					if (conn != null) {
						conn.close();
						conn = null;
					}
				} catch (SQLException e) {
					throw e;
				}
			}
		}

	}


    private static void search(String text) throws IOException {

        Path path = Paths.get("/Users/German/tmp/lucene/indexes");
        Directory directory = FSDirectory.open(path);
        //IndexReader indexReader =  DirectoryReader.open(directory);

        /*
         * SearcherManager
			Utility class to safely share IndexSearcher instances across multiple threads, while periodically reopening.
			This class ensures each searcher is closed only once all threads have finished using it.
         */

        // TODO: Factoria que debemos generar de forma correcta por performance.
        // no se ve tan necesario hacer esta factoria por ahora... observar stress test.
        // por lo visto, autosuggest lo hace por debajo.
        ReferenceManager<IndexSearcher> searcherManager = new SearcherManager(directory, new SearcherFactory() );
        searcherManager.maybeRefresh();
        IndexSearcher indexSearcher = searcherManager.acquire(); //deberia ser un Singleton !


        //paginacion:
        int showing_tips = 10;

        try {

            QueryParser queryParser = new QueryParser("contents",  new StandardAnalyzer());
            Query query = queryParser.parse(text);

	        TopDocs topDocs = indexSearcher.search(query, showing_tips);

	        System.out.println("totalHits " + topDocs.totalHits);
	        System.out.println("mostrando los " + showing_tips + " primeros:");

	        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
	            Document document = indexSearcher.doc(scoreDoc.doc);
	            System.out.println("content: " + document.get("contents"));
	        }
        } catch (ParseException e) {
        	e.printStackTrace();
		} finally {
	      	searcherManager.release(indexSearcher);

	        // Set to null to ensure we never again try to use
	        // this searcher instance after releasing:
	    	indexSearcher = null;
        }

    }


}
