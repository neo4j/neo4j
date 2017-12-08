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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.Suppliers;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DeserializerFactories.defaultNodeDeserializer;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.INTEGER;

public class InputGroupsDeserializerTest
{
    @Test
    public void shouldBeAbleToAskForSourceInformationEvenBetweenTwoSources() throws Exception
    {
        // GIVEN
        List<DataFactory<InputNode>> data = asList( data( ":ID\n1" ), data( "2" ) );
        final AtomicInteger flips = new AtomicInteger();
        final AtomicReference<InputGroupsDeserializer<InputNode>> deserializerTestHack = new AtomicReference<>( null );
        InputGroupsDeserializer<InputNode> deserializer = new InputGroupsDeserializer<>(
                data.iterator(), defaultFormatNodeFileHeader(), lowBufferSize( COMMAS, true ), INTEGER,
                Runtime.getRuntime().availableProcessors(), 1, ( header, stream, decorator, validator ) ->
                {
                    // This is the point where the currentInput field in InputGroupsDeserializer was null
                    // so ensure that's no longer the case, just by poking those source methods right here and now.
                    if ( flips.get() == 0 )
                    {
                        assertNotNull( deserializerTestHack.get().sourceDescription() );
                    }
                    else
                    {
                        assertEquals( "" + flips.get(), deserializerTestHack.get().sourceDescription() );
                    }

                    flips.incrementAndGet();
                    @SuppressWarnings( "unchecked" )
                    InputEntityDeserializer<InputNode> result = mock( InputEntityDeserializer.class );
                    when( result.sourceDescription() ).thenReturn( String.valueOf( flips.get() ) );
                    doAnswer( invocation ->
                    {
                        stream.close();
                        return null;
                    } ).when( result ).close();
                    return result;
                }, Validators.emptyValidator(), InputNode.class );
        deserializerTestHack.set( deserializer );

        // WHEN running through the iterator
        count( deserializer );

        // THEN there should have been two data source flips
        assertEquals( 2, flips.get() );
        deserializer.close();
    }

    @Test
    public void shouldCoordinateGroupCreationForParallelProcessing() throws Exception
    {
        // GIVEN
        List<DataFactory<InputNode>> data = new ArrayList<>();
        int processors = Runtime.getRuntime().availableProcessors();
        for ( int i = 0; i < processors; i++ )
        {
            StringBuilder builder = new StringBuilder( ":ID(Group" + i + ")" );
            for ( int j = 0; j < 100; j++ )
            {
                builder.append( "\n" + j );
            }
            data.add( data( builder.toString() ) );
        }
        Groups groups = new Groups();
        IdType idType = IdType.INTEGER;
        Collector badCollector = mock( Collector.class );
        Configuration config = lowBufferSize( COMMAS, false );
        DeserializerFactory<InputNode> factory = defaultNodeDeserializer( groups, config, idType, badCollector );
        try ( InputGroupsDeserializer<InputNode> deserializer = new InputGroupsDeserializer<>(
                data.iterator(), defaultFormatNodeFileHeader(), config, idType,
                processors, processors, factory, Validators.emptyValidator(), InputNode.class ) )
        {
            // WHEN
            count( deserializer );
        }

        // THEN
        assertEquals( processors, groups.getOrCreate( "LastOne" ).id() );
        boolean[] seen = new boolean[processors];
        for ( int i = 0; i < processors; i++ )
        {
            String groupName = "Group" + i;
            groups.getOrCreate( groupName );
            assertFalse( seen[i] );
            seen[i] = true;
        }
    }

    private Configuration lowBufferSize( Configuration conf, boolean multilineFields )
    {
        return new Configuration.Overridden( conf )
        {
            @Override
            public int bufferSize()
            {
                return 100;
            }

            @Override
            public boolean multilineFields()
            {
                return multilineFields;
            }
        };
    }

    private DataFactory<InputNode> data( String string )
    {
        return DataFactories.data( NO_NODE_DECORATOR, Suppliers.singleton( wrap( string ) ) );
    }
}
