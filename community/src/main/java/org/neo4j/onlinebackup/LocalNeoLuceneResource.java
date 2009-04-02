package org.neo4j.onlinebackup;

import java.io.File;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.LuceneIndexService;

/**
 * Start an EmbeddedNeo from a directory location together with an
 * LuceneIndexService and wrap it as XA data source.
 */
public class LocalNeoLuceneResource extends EmbeddedNeoResource
{
    private final IndexService lucene;

    private LocalNeoLuceneResource( EmbeddedNeo neo )
    {
        super( neo );
        this.lucene = new LuceneIndexService( neo );
    }

    public static LocalNeoLuceneResource getInstance( String storeDir )
    {
        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + "neostore";
        if ( !new File( store ).exists() )
        {
            throw new RuntimeException( "Unable to locate local neo store in["
                + storeDir + "]" );
        }
        return new LocalNeoLuceneResource( new EmbeddedNeo( storeDir ) );
    }

    @Override
    public void close()
    {
        lucene.shutdown();
        neo.shutdown();
    }
}
