package org.neo4j.kernel.impl.ha;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.helpers.Pair;

public class TransactionStreams
{
    private final Collection<Pair<String, TransactionStream>> streams =
            new ArrayList<Pair<String,TransactionStream>>();
    
    public void add( String resource, TransactionStream stream )
    {
        this.streams.add( new Pair<String, TransactionStream>( resource, stream ) );
    }
    
    public Collection<Pair<String, TransactionStream>> getStreams()
    {
        return Collections.unmodifiableCollection( this.streams );
    }
}
