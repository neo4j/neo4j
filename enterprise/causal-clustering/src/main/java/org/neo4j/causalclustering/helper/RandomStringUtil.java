package org.neo4j.causalclustering.helper;

import java.util.UUID;

import static java.lang.Integer.min;

public class RandomStringUtil
{
    public static String generateId()
    {
        return generateId( 5 );
    }

    public static String generateId( int maxLength )
    {
        String string = UUID.randomUUID().toString();
        return string.substring( 0, min( maxLength, string.length() ) );
    }
}
