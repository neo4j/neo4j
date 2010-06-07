package org.neo4j.shell;

import java.util.ArrayList;
import java.util.Collection;
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
                Object value = data.get( key );
                if ( value != null )
                {
                    String replacement = data.get( key ).toString();
                    String regExpMatchString = variablePrefix + key;
                    result = result.replaceAll( regExpMatchString, replacement );
                }
            }
        }
        
        return result;
    }
    
    public static String lastWordOrQuoteOf( String text, boolean preserveQuotation )
    {
        String[] quoteParts = text.split( "\"" );
        String lastPart = quoteParts[quoteParts.length-1];
        boolean isWithinQuotes = quoteParts.length % 2 == 0;
        String lastWord = null;
        if ( isWithinQuotes )
        {
            lastWord = lastPart;
            if ( preserveQuotation )
            {
                lastWord = "\"" + lastWord + (text.endsWith( "\"" ) ? "\"" : "" );
            }
        }
        else
        {
            String[] lastPartParts = splitAndKeepEscapedSpaces( lastPart, preserveQuotation );
            lastWord = lastPartParts[lastPartParts.length-1];
        }
        return lastWord;
    }

    public static String[] splitAndKeepEscapedSpaces( String string, boolean preserveEscapes )
    {
        Collection<String> result = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        for ( int i = 0; i < string.length(); i++ )
        {
            char ch = string.charAt( i );
            if ( ch == ' ' )
            {
                boolean isGluedSpace = i > 0 && string.charAt( i-1 ) == '\\';
                if ( !isGluedSpace )
                {
                    result.add( current.toString() );
                    current = new StringBuilder();
                    continue;
                }
            }
            
            if ( preserveEscapes || ch != '\\' )
            {
                current.append( ch );
            }
        }
        if ( current.length() > 0 )
        {
            result.add( current.toString() );
        }
        return result.toArray( new String[result.size()] );
    }
}
