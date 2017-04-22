/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;

import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.CachingInputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;

public class CsvOutput implements BatchImporter
{
    private final File targetDirectory;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Deserialization<String> deserialization;

    public CsvOutput( File targetDirectory, Header nodeHeader, Header relationshipHeader, Configuration config )
    {
        this.targetDirectory = targetDirectory;
        assert targetDirectory.isDirectory();
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.deserialization = new StringDeserialization( config );
        targetDirectory.mkdirs();
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        Function<CachingInputEntityVisitor,String> deserializer = (entity) ->
        {
            deserialization.clear();
            for ( Header.Entry entry : nodeHeader.entries() )
            {
                switch ( entry.type() )
                {
                case ID:
                    deserialization.handle( entry, entity.hasLongId ? entity.longId : entity.stringId );
                    break;
                case PROPERTY:
                    deserialization.handle( entry, property( entity.properties, entry.name() ) );
                    break;
                case LABEL:
                    deserialization.handle( entry, entity.labels.toArray( new String[entity.labels.size()] ) );
                    break;
                case TYPE:
                    deserialization.handle( entry, entity.hasIntType ? entity.intType : entity.stringType );
                    break;
                case START_ID:
                    deserialization.handle( entry, entity.hasLongStartId ? entity.longStartId : entity.stringStartId );
                    break;
                case END_ID:
                    deserialization.handle( entry, entity.hasLongEndId ? entity.longEndId : entity.stringEndId );
                    break;
                default: // ignore other types
                }
            }
            return deserialization.materialize();
        };
        consume( "nodes.csv", input.nodes(), nodeHeader, deserializer );
        consume( "relationships.csv", input.relationships(), relationshipHeader, deserializer );
    }

    private Object property( List<Object> properties, String key )
    {
        for ( int i = 0; i < properties.size(); i += 2 )
        {
            if ( properties.get( i ).equals( key ) )
            {
                return properties.get( i + 1 );
            }
        }
        return null;
    }

    private void consume( String name, InputIterable entities, Header header,
            Function<CachingInputEntityVisitor,String> deserializer ) throws IOException
    {
        try ( PrintStream out = file( name ) )
        {
            serialize( out, header );
            try ( InputIterator iterator = entities.iterator();
                    InputChunk chunk = iterator.newChunk() )
            {
                CachingInputEntityVisitor visitor = new CachingInputEntityVisitor();
                while ( iterator.next( chunk ) )
                {
                    while ( chunk.next( visitor ) )
                    {
                        out.println( deserializer.apply( visitor ) );
                    }
                }
            }
        }
    }

    private void serialize( PrintStream out, Header header )
    {
        deserialization.clear();
        for ( Header.Entry entry : header.entries() )
        {
            deserialization.handle( entry, entry.toString() );
        }
        out.println( deserialization.materialize() );
    }

    private PrintStream file( String name ) throws IOException
    {
        return new PrintStream( new File( targetDirectory, name ) );
    }
}
