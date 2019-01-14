/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.StringDeserialization;

import static org.neo4j.io.ByteUnit.mebiBytes;

public class CsvOutput implements BatchImporter
{
    private interface Deserializer
    {
        String apply( InputEntity entity, Deserialization<String> deserialization, Header header );
    }

    private final File targetDirectory;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private Configuration config;
    private final Deserialization<String> deserialization;

    public CsvOutput( File targetDirectory, Header nodeHeader, Header relationshipHeader, Configuration config )
    {
        this.targetDirectory = targetDirectory;
        assert targetDirectory.isDirectory();
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.config = config;
        this.deserialization = new StringDeserialization( config );
        targetDirectory.mkdirs();
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        Deserializer deserializer = ( entity, deserialization, header ) ->
        {
            deserialization.clear();
            for ( Header.Entry entry : header.entries() )
            {
                switch ( entry.type() )
                {
                case ID:
                    deserialization.handle( entry, entity.hasLongId ? entity.longId : entity.objectId );
                    break;
                case PROPERTY:
                    deserialization.handle( entry, property( entity.properties, entry.name() ) );
                    break;
                case LABEL:
                    deserialization.handle( entry, entity.labels() );
                    break;
                case TYPE:
                    deserialization.handle( entry, entity.hasIntType ? entity.intType : entity.stringType );
                    break;
                case START_ID:
                    deserialization.handle( entry, entity.hasLongStartId ? entity.longStartId : entity.objectStartId );
                    break;
                case END_ID:
                    deserialization.handle( entry, entity.hasLongEndId ? entity.longEndId : entity.objectEndId );
                    break;
                default: // ignore other types
                }
            }
            return deserialization.materialize();
        };
        consume( "nodes", input.nodes().iterator(), nodeHeader, deserializer );
        consume( "relationships", input.relationships().iterator(), relationshipHeader, deserializer );
    }

    private static Object property( List<Object> properties, String key )
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

    private void consume( String name, InputIterator entities, Header header, Deserializer deserializer ) throws IOException
    {
        try ( PrintStream out = file( name + "header.csv" ) )
        {
            serialize( out, header );
        }

        try
        {
            int threads = Runtime.getRuntime().availableProcessors();
            ExecutorService executor = Executors.newFixedThreadPool( threads );
            for ( int i = 0; i < threads; i++ )
            {
                int id = i;
                executor.submit( (Callable<Void>) () -> {
                    StringDeserialization deserialization = new StringDeserialization( config );
                    try ( PrintStream out = file( name + "-" + id + ".csv" );
                          InputChunk chunk = entities.newChunk() )
                    {
                        InputEntity entity = new InputEntity();
                        while ( entities.next( chunk ) )
                        {
                            while ( chunk.next( entity ) )
                            {
                                out.println( deserializer.apply( entity, deserialization, header ) );
                            }
                        }
                    }
                    return null;
                } );
            }
            executor.shutdown();
            executor.awaitTermination( 10, TimeUnit.MINUTES );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IOException( e );
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
        return new PrintStream( new BufferedOutputStream( new FileOutputStream( new File( targetDirectory, name ) ),
                (int) mebiBytes( 1 ) ) );
    }
}
