package examples;
import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

public class TakeItForASpin
{
    public static void main( String[] args ) throws IOException
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( cleared( "the-path" ) );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private static String cleared( String string ) throws IOException
    {
        File directory = new File( string ).getAbsoluteFile();
        FileUtils.deleteRecursively( directory );
        return directory.getAbsolutePath();
    }
}
