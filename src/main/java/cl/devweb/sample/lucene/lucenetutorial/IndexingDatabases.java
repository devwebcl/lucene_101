package cl.devweb.sample.lucene.lucenetutorial;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class IndexingDatabases {

	private static String ruta = "/Users/German/tmp/lucene/indexes";


	public static void main(String[] args) throws SQLException, IOException {

		System.out.println("Start.\n"+(new Date())+"\n");
		indexDatabase();
		System.out.println("Fin indexing.\n"+(new Date())+"\n");

		search("torres");
		System.out.println("\n"+(new Date())+"\n");
	}


	private static void indexDatabase() throws IOException, SQLException {

        //indexing directory
        Path path = Paths.get(ruta);
        Directory directory = FSDirectory.open(path);

        IndexWriterConfig config = new IndexWriterConfig(new SimpleAnalyzer());
        config.setCodec(new Lucene54Codec(Mode.BEST_SPEED));  // BEST_SPEED  BEST_COMPRESSION

        IndexWriter indexWriter = new IndexWriter(directory, config);
        //indexWriter.deleteAll();


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
			javax.sql.DataSource ds = (javax.sql.DataSource) ctx.lookup("JDBC/VISTA_360_CN_CT");  // JDBC/VISTA_360_CN    JDBC/VISTA_360_CN_CT
			conn = ds.getConnection();
			conn.setAutoCommit(true);

			long tamanno = 0;
			stmt = conn.createStatement();

			// query mas representativa:
			String sql = "SELECT\n" +
					"sc.FST_NAME, sc.LAST_NAME, sc.PERSON_UID, LISTAGG(sa.asset_num, ' ') WITHIN GROUP (ORDER BY sc.ROW_ID) \"asset_num\" \n" +
					"FROM S_CONTACT sc\n" +
					"INNER JOIN S_ASSET_CON sac ON sc.PAR_ROW_ID = sac.CONTACT_ID\n" +
					"INNER JOIN S_ASSET sa ON sa.ROW_ID = sac.ASSET_ID \n" +
					"INNER JOIN S_PROD_INT spi ON spi.ROW_ID = sa.PROD_ID \n" +
					"WHERE sc.PERSON_UID != '1-9'\n" +
					"GROUP BY sc.ROW_ID, sc.FST_NAME, sc.LAST_NAME, sc.PERSON_UID";
			// original:
			//String sql = "SELECT sc.person_uid, sc.fst_name, sc.last_name, sc.mid_name FROM vista_360_cn.s_contact sc";
			//sql = sql + " WHERE last_name = 'Torres'";
			// Loop:
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				Document doc = new Document();
				String s = rs.getString("FST_NAME")  + " " +  rs.getString("LAST_NAME") //prototipo de estructura de dato;
				 +  " " + rs.getString("PERSON_UID") +  " " + rs.getString("asset_num");
				//System.out.println("s=" + s );

				if(s != null) {
					tamanno = tamanno + s.length();
					doc.add(new TextField("contents", s, Store.YES));
				}
				indexWriter.addDocument(doc);
			}

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


    private static void search(String text) {

        try {
            Path path = Paths.get(ruta);
            Directory directory = FSDirectory.open(path);
            IndexReader indexReader =  DirectoryReader.open(directory);
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            QueryParser queryParser = new QueryParser("contents",  new StandardAnalyzer());
            Query query = queryParser.parse(text);

            //paginacion:
            int showing_tips = 10;

            TopDocs topDocs = indexSearcher.search(query, showing_tips);

            System.out.println("totalHits " + topDocs.totalHits);

            System.out.println("mostrando los " + showing_tips + " primeros:");

            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document document = indexSearcher.doc(scoreDoc.doc);
                System.out.println("content: " + document.get("contents"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
