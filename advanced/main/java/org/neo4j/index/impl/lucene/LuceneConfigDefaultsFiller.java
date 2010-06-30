package org.neo4j.index.impl.lucene;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.index.impl.DefaultsFiller;

public class LuceneConfigDefaultsFiller implements DefaultsFiller
{
    public static final LuceneConfigDefaultsFiller INSTANCE = new LuceneConfigDefaultsFiller();
    
    public Map<String, String> fill( Map<String, String> source )
    {
        Map<String, String> result = source != null ?
                new HashMap<String, String>( source ) : new HashMap<String, String>();
        String type = result.get( "type" );
        if ( type == null )
        {
            type = "exact";
            result.put( "type", type );
        }
        if ( type.equals( "fulltext" ) )
        {
            if ( !result.containsKey( "to_lower_case" ) )
            {
                result.put( "to_lower_case", "true" );
            }
        }
        return result;
    }
}
