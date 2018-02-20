package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.queryparser.classic.QueryParser;

import java.util.Collection;

import static java.util.stream.Collectors.joining;

public class FulltextQueryHelper
{
    public static String createQuery( Collection<String> query, boolean fuzzy, boolean matchAll )
    {
        String delim = "";
        if ( matchAll )
        {
            delim = "&& ";
        }
        if ( fuzzy )
        {
            return query.stream().map( QueryParser::escape ).collect( joining( "~ " + delim, "", "~" ) );
        }
        return query.stream().map( QueryParser::escape ).collect( joining( " " + delim ) );
    }
}
