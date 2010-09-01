package org.neo4j.kernel.ha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.helpers.Pair;

public class TransactionStreams
{
    public static final TransactionStreams EMPTY = new TransactionStreams()
    {
        @Override
        public void add( String resource, TransactionStream stream )
        {
            throw new UnsupportedOperationException( "read-only instance" );
        }
    };
    
    private List<Pair<String, TransactionStream>> streams;
    
    public void add( String resource, TransactionStream stream )
    {
        ensureListInstantiated();
        this.streams.add( new Pair<String, TransactionStream>( resource, stream ) );
    }
    
    public Collection<Pair<String, TransactionStream>> getStreams()
    {
        return streams == null ? Collections.<Pair<String, TransactionStream>>emptyList() :
                Collections.unmodifiableList( streams );
    }
    
    private void ensureListInstantiated()
    {
        if ( streams == null )
        {
            streams = new ArrayList<Pair<String,TransactionStream>>();
        }
    }

    public void close() throws IOException
    {
        if ( streams != null )
        {
            for ( Pair<String, TransactionStream> stream : streams )
            {
                stream.other().close();
            }
        }
    }
}
