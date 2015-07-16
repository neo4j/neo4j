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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.function.Function;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.COMMENTS;
import static java.util.regex.Pattern.DOTALL;

/**
 * Node (signature=0x4E) {
 * Identity          identity
 * List<Text>        labels
 * Map<Text, Value>  properties
 * }
 */
public class DocStruct implements Iterable<DocStruct.Field>
{
    public static Function<Element,DocStruct> struct_definition = new Function<Element,DocStruct>()
    {
        @Override
        public DocStruct apply( Element s ) throws RuntimeException
        {
            return new DocStruct( s.text() );
        }
    };

    private final String name;
    private final Map<String,String> attributes;
    private final List<Field> fields;
    private final String raw;

    public static class Field
    {
        private final String typeSignature;
        private final String name;
        private final String exampleValue;

        public Field( String typeSignature, String name, String exampleValue )
        {
            this.typeSignature = typeSignature;
            this.name = name;
            this.exampleValue = exampleValue;
        }

        @Override
        public String toString()
        {
            return typeSignature + "  " + name;
        }

        public String type()
        {
            return typeSignature;
        }

        public String name()
        {
            return name;
        }

        public String exampleValueOr( String defaultValue )
        {
            return exampleValue == null ? defaultValue : exampleValue;
        }
    }

    public static final Pattern STRUCT_PATTERN = Pattern.compile(
            "^(?<name>[a-z]+)\\s+                 # StructName \n" +
            "     \\( (?<attrs>[^)]+) \\) \\s*    # (identifier=0x01,..) \n" +
            "\\{                                  # \\{\n" +
            "\\s*    (?<fields>[^\\}]+)           #     Type fieldName..\n" +
            "\\}\\s*                              # \\}\n" +
            "(.*)$" +
            "", CASE_INSENSITIVE | COMMENTS | DOTALL );

    public DocStruct( String structDefinition )
    {
        Matcher matcher = STRUCT_PATTERN.matcher( structDefinition );
        if ( !matcher.matches() )
        {
            throw new RuntimeException( "Unable to parse struct definition: \n" + structDefinition );
        }
        this.raw = structDefinition;
        this.name = matcher.group( "name" );
        this.attributes = parseAttributes( matcher.group( "attrs" ) );
        this.fields = parseFields( matcher.group( "fields" ) );
    }

    public String name()
    {
        return name;
    }

    public String attribute( String key )
    {
        return attributes.get( key );
    }

    @Override
    public Iterator<Field> iterator()
    {
        return fields.iterator();
    }

    private List<Field> parseFields( String raw )
    {
        List<Field> out = new LinkedList<>();
        for ( String s : raw.split( "\n" ) )
        {
            String[] parts = s.trim().split( "\\s+", 2 );
            String[] valAndExample = parts[1].split( "//\\se\\.g\\." );

            String example = valAndExample.length == 2 ?
                    valAndExample[1].replaceAll("^\"+", "").replaceAll("\"+$", "") : null;
            out.add( new Field( parts[0], valAndExample[0], example ) );
        }
        return out;
    }

    public static Map<String,String> parseAttributes( String raw )
    {
        Map<String,String> out = new HashMap<>();
        for ( String attr : raw.split( "," ) )
        {
            String[] split = attr.split( "=" );
            out.put( split[0].trim(), split[1].trim() );
        }
        return out;
    }

    @Override
    public String toString()
    {
        return raw;
    }

    public byte signature()
    {
        return (byte) Integer.parseInt( attribute( "signature" ).substring( 2 ), 16 );
    }

    public int size()
    {
        return fields.size();
    }
}
