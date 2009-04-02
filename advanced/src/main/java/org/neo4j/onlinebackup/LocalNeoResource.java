package org.neo4j.onlinebackup;

import java.io.File;

import org.neo4j.api.core.EmbeddedNeo;

/**
 * Start an EmbeddedNeo from a directory location and wrap it as XA data source.
 */
public class LocalNeoResource extends EmbeddedNeoResource
{
    private LocalNeoResource( EmbeddedNeo neo )
    {
        super( neo );
    }

    public static LocalNeoResource getInstance( String storeDir )
    {
        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + "neostore";
        if ( !new File( store ).exists() )
        {
            throw new RuntimeException( "Unable to locate local neo store in["
                + storeDir + "]" );
        }
        return new LocalNeoResource( new EmbeddedNeo( storeDir ) );
    }
}
