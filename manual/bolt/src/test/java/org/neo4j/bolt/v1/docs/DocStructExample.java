/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.docs;

import org.jsoup.nodes.Element;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static org.neo4j.bolt.v1.docs.DocPartParser.Decoration.withDetailedExceptions;

/**
 * Unlike {@link DocStruct}, this represents a "real" example struct with values, rather than the blueprint for one.
 */
public class DocStructExample implements Iterable<String>
{
    public static DocPartParser<DocStructExample> struct_example =
        withDetailedExceptions( DocStructExample.class, new DocPartParser<DocStructExample>()
            {
                @Override
                public DocStructExample parse( String fileName, String title, Element s )
                {
                    return new DocStructExample( DocPartName.create( fileName, title ), s.text() );
                }
            }
        );

    private final DocPartName name;
    private final Map<String,String> attributes;
    private final List<String> fields;
    private final String raw;

    public DocStructExample( DocPartName name, String structDefinition )
    {
        Matcher matcher = DocStruct.STRUCT_PATTERN.matcher( structDefinition );
        if ( !matcher.matches() )
        {
            throw new RuntimeException( "Unable to parse struct definition: \n" + structDefinition );
        }
        this.raw = structDefinition;
        this.name = name.withDetail( matcher.group( "name" ) );
        this.attributes = DocStruct.parseAttributes( matcher.group( "attrs" ) );
        this.fields = parseFields( matcher.group( "fields" ) );
    }

    public String name()
    {
        return name.toString();
    }

    public String attribute( String key )
    {
        return attributes.get( key );
    }

    @Override
    public Iterator<String> iterator()
    {
        return fields.iterator();
    }

    private List<String> parseFields( String raw )
    {
        List<String> out = new LinkedList<>();
        for ( String s : raw.split( "," ) )
        {
            out.add( s.trim() );
        }
        return out;
    }

    @Override
    public String toString()
    {
        return raw;
    }

    public int signature()
    {
        return Integer.parseInt( attribute( "signature" ).substring( 2 ), 16 );
    }

    public int size()
    {
        return fields.size();
    }
}
