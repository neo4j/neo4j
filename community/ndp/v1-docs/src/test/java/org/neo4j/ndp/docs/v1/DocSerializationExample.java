/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ndp.docs.v1;

import org.jsoup.nodes.Element;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Function;

/**
 * Value: 1.1
 * <p/>
 * C1 3F F1 99 99 99 99 99 9A
 */
public class DocSerializationExample
{
    public static Function<Element,DocSerializationExample> serialization_example
            = new Function<Element,DocSerializationExample>()
    {
        @Override
        public DocSerializationExample apply( Element s ) throws RuntimeException
        {
            return new DocSerializationExample( s.text() );
        }
    };

    private final Map<String,String> attributes = new HashMap<>();
    private final String raw;
    private String serializedData = "";

    public DocSerializationExample( String raw )
    {
        this.raw = raw;
        boolean readingHeaders = true;
        for ( String line : raw.split( "\n" ) )
        {
            if ( line.trim().equals( "" ) )
            {
                // Blank line denotes header/binary data body split
                readingHeaders = false;
            }
            else if ( readingHeaders )
            {
                String[] split = line.split( ":", 2 );
                attributes.put( split[0].trim(), split[1].trim() );
            }
            else
            {
                if ( line.matches( "^[a-fA-f0-9\\s]+$" ) )
                {
                    serializedData += line;
                }
            }
        }
    }

    public String attribute( String name )
    {
        return attributes.get( name );
    }

    public String serializedData()
    {
        return serializedData;
    }

    @Override
    public String toString()
    {
        return raw;
    }
}
