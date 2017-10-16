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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.UpdateBehaviour;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static java.lang.String.valueOf;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.nodeData;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.relationshipData;

public class ExternalPropertiesDecoratorIT
{
    @Test
    public void shouldDecorateExternalPropertiesInParallelProcessingCsvInput() throws Exception
    {
        // GIVEN
        int processors = 5;
        Collector collector = mock( Collector.class );
        int count = 1000;
        Configuration config = new Configuration.Overridden( Configuration.COMMAS )
        {
            @Override
            public int bufferSize()
            {
                // Keep this low so that there will be many batches, to exercise the parallel processing
                // 300 is empirically measured to roughly produce ~20 chunks
                return 300;
            }
        };
        IdType idType = IdType.STRING;
        Decorator<InputNode> decorator = spy( new ExternalPropertiesDecorator(
                data( NO_NODE_DECORATOR, () -> decoratedData( count ) ),
                defaultFormatNodeFileHeader(),
                config, idType, UpdateBehaviour.ADD, collector ) );
        Input input = new CsvInput(
                nodeData( data( decorator, () -> mainData( count ) ) ), defaultFormatNodeFileHeader(),
                relationshipData(), defaultFormatRelationshipFileHeader(),
                idType, config,
                collector, processors, true );

        // WHEN/THEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            int i = 0;
            for ( ; i < count; i++ )
            {
                assertTrue( nodes.hasNext() );
                InputNode node = nodes.next();
                // This property comes from decorator
                assertHasProperty( node, "extra", node.id() + "-decorated" );
                if ( i == 0 )
                {
                    // This code is equal to nodes.setProcessors( processors ) (a method which doesn't exist)
                    nodes.processors( processors - nodes.processors( 0 ) );
                }
            }
            assertEquals( count, i );
            assertFalse( nodes.hasNext() );
        }
        verify( decorator ).close();
    }

    private void assertHasProperty( InputNode node, String key, Object value )
    {
        Object[] properties = node.properties();
        boolean found = false;
        for ( int i = 0; i < properties.length; i++ )
        {
            if ( properties[i++].toString().equals( key ) )
            {
                assertFalse( found );
                found = true;
                assertEquals( value, properties[i] );
            }
        }
        assertTrue( found );
    }

    String id( int i )
    {
        return valueOf( i );
    }

    private CharReadable decoratedData( int count )
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try ( PrintStream printer = new PrintStream( out ) )
        {
            printer.println( ":ID,extra" );
            for ( int i = 0; i < count; i++ )
            {
                printer.println( id( i ) + "," + id( i ) + "-decorated" );
            }
        }
        return wrap( out.toString() );
    }

    private CharReadable mainData( int count )
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try ( PrintStream printer = new PrintStream( out ) )
        {
            printer.println( ":ID" );
            for ( int i = 0; i < count; i++ )
            {
                printer.println( id( i ) );
            }
        }
        return wrap( out.toString() );
    }
}
