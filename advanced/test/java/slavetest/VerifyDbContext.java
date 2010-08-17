package slavetest;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

class VerifyDbContext
{
    final GraphDatabaseService db;
    final IndexService index;
    
    public VerifyDbContext( GraphDatabaseService db )
    {
        this.db = db;
        this.index = db instanceof HighlyAvailableGraphDatabase ?
                ((HighlyAvailableGraphDatabase) db).getIndexService() : new LuceneIndexService( db );
    }
}
