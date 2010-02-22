package org.neo4j.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextUtil
{
    public static String templateString( String templateString,
            Map<String, ? extends Object> data )
    {
        return templateString( templateString, "\\$", data );
    }
    
    public static String templateString( String templateString,
            String variablePrefix, Map<String, ? extends Object> data )
    {
        // Sort data strings on length.
        Map<Integer, List<String>> lengthMap =
            new HashMap<Integer, List<String>>();
        int longest = 0;
        for ( String key : data.keySet() )
        {
            int length = key.length();
            if ( length > longest )
            {
                longest = length;
            }
            
            List<String> innerList = null;
            Integer innerKey = Integer.valueOf( length );
            if ( lengthMap.containsKey( innerKey ) )
            {
                innerList = lengthMap.get( innerKey );
            }
            else
            {
                innerList = new ArrayList<String>();
                lengthMap.put( innerKey, innerList );
            }
            innerList.add( key );
        }
        
        // Replace it.
        String result = templateString;
        for ( int i = longest; i >= 0; i-- )
        {
            Integer lengthKey = Integer.valueOf( i );
            if ( !lengthMap.containsKey( lengthKey ) )
            {
                continue;
            }
            
            List<String> list = lengthMap.get( lengthKey );
            for ( String key : list )
            {
                String replacement = data.get( key ).toString();
                String regExpMatchString = variablePrefix + key;
                result = result.replaceAll( regExpMatchString, replacement );
            }
        }
        
        return result;
    }
}
