/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.function.Suppliers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.INTEGER;

public class InputGroupsDeserializerTest
{
    @Test
    public void shouldBeAbleToAskForSourceInformationEvenBetweenTwoSources() throws Exception
    {
        // GIVEN
        List<DataFactory<InputNode>> data = asList( data( ":ID\n1" ), data( "2" ) );
        final AtomicInteger flips = new AtomicInteger();
        InputGroupsDeserializer<InputNode> deserializer = new InputGroupsDeserializer<InputNode>(
                data.iterator(), defaultFormatNodeFileHeader(), lowBufferSize( COMMAS ), INTEGER )
        {
            @Override
            protected InputEntityDeserializer<InputNode> entityDeserializer( CharSeeker dataStream, Header dataHeader,
                    Function<InputNode,InputNode> decorator )
            {
                // This is the point where the currentInput field in InputGroupsDeserializer was null
                // so ensure that's no longer the case, just by poking those source methods right here and now.
                if ( flips.get() == 0 )
                {
                    assertNotNull( sourceDescription() );
                }
                else
                {
                    assertEquals( "" + flips.get(), sourceDescription() );
                }

                flips.incrementAndGet();
                @SuppressWarnings( "unchecked" )
                InputEntityDeserializer<InputNode> result = mock( InputEntityDeserializer.class );
                when( result.sourceDescription() ).thenReturn( String.valueOf( flips.get() ) );
                return result;
            }
        };

        // WHEN running through the iterator
        count( deserializer );

        // THEN there should have been two data source flips
        assertEquals( 2, flips.get() );
    }

    private Configuration lowBufferSize( Configuration conf )
    {
        return new Configuration.Overriden( conf )
        {
            @Override
            public int bufferSize()
            {
                return 100;
            }
        };
    }

    private DataFactory<InputNode> data( String string )
    {
        return DataFactories.data( NO_NODE_DECORATOR, Suppliers.singleton( wrap( new StringReader( string ) ) ) );
    }
}
