import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;

public class HelloWorldCassandra {
    public static void main (String args[]){
        System.out.println("Hello, Cassandra");
        Cluster cluster = null;
        try{
            cluster = Cluster.builder()
                    .addContactPoint("localhost")
                    .build();
            Session session = cluster.connect();
            ResultSet rs = session.execute ("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            KeyspaceRepository sr = new KeyspaceRepository(session);
            sr.createKeyspace("library","SimpleStrategy", 1);
            System.out.println("create repository");

            sr.useKeyspace("library");
            System.out.println("Using repository library");

            BookRepository br = new BookRepository(session);
            br.createTable();
            System.out.println("Create table book");
            
            br.alterTablebooks("publisher", "text");

            System.out.println("Alter table book");
            br.createTableBooksByTitle();
            System.out.println("Create Table book - step 2");

            Book book = new Book (UUIDs.timeBased(), "Effective Java 1", "Joshua Bloch a", "Programming 1");
            br.insertBookBatch(book);
            Book book2 = new Book (UUIDs.timeBased(), "Effective Java 2", "Joshua Bloch b", "Programming 2");
            br.insertBookBatch(book2);
            System.out.println("Insert Books");

            br.selectAll();

            br.deletebookByTitle("Effective Java");

            br.deleteTable("booksByTitle");
            System.out.println("Delete Books");


            br.deleteTable("books");

            sr.deleteKeyspace("library");
            System.out.println("Delete keyspace library");

        }finally{
            if (cluster != null) cluster.close();
        }
    }
}
