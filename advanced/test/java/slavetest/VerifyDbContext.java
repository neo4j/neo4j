package slavetest;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.index.impl.lucene.LuceneIndexProvider;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

class VerifyDbContext
{
    final GraphDatabaseService db;
    // final IndexService indexService;
    final IndexProvider indexProvider;
    
    public VerifyDbContext( GraphDatabaseService db )
    {
        this.db = db;
//        this.indexService = db instanceof HighlyAvailableGraphDatabase ?
//                ((HighlyAvailableGraphDatabase) db).getIndexService() : new LuceneIndexService( db );
        this.indexProvider = db instanceof HighlyAvailableGraphDatabase ?
                ((HighlyAvailableGraphDatabase) db).getIndexProvider() : new LuceneIndexProvider( db );
    }
}
