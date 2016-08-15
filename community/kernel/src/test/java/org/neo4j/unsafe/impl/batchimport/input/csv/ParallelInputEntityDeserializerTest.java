/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.ACTUAL;

public class ParallelInputEntityDeserializerTest
{
    @Rule
    public final RandomRule random = new RandomRule().withSeed( 1468928804595L );

    @Test
    public void shouldParseDataInParallel() throws Exception
    {
        // GIVEN
        int entities = 500;
        Data<InputNode> data = testData( entities );
        Configuration config = new Configuration.Overriden( COMMAS )
        {
            @Override
            public int bufferSize()
            {
                return 100;
            }
        };
        IdType idType = ACTUAL;
        Collector badCollector = mock( Collector.class );
        Groups groups = new Groups();
        Set<Thread> observedProcessingThreads = new CopyOnWriteArraySet<>();
        int threads = 4;
        DeserializerFactory<InputNode> deserializerFactory = (chunk,header,decorator) ->
        {
            observedProcessingThreads.add( Thread.currentThread() );
            // Make sure there will be 4 different processing threads doing this
            boolean allThreadsStarted;
            do
            {
                allThreadsStarted = observedProcessingThreads.size() == threads;
            }
            while ( !allThreadsStarted );
            return new InputEntityDeserializer<>( header, chunk, config.delimiter(),
                    new InputNodeDeserialization( chunk, header, groups, idType.idsAreExternal() ), decorator,
                    Validators.<InputNode>emptyValidator(), badCollector );
        };
        try ( ParallelInputEntityDeserializer<InputNode> deserializer = new ParallelInputEntityDeserializer<>( data,
                defaultFormatNodeFileHeader(), config, idType, threads, deserializerFactory, InputNode.class ) )
        {
            deserializer.processors( threads );

            // WHEN/THEN
            long previousLineNumber = -1;
            long previousPosition = -1;
            for ( long i = 0; i < entities; i++ )
            {
                assertTrue( deserializer.hasNext() );
                InputNode entity = deserializer.next();
                assertEquals( i, ((Long) entity.id()).longValue() );
                assertEquals( "name", entity.properties()[0] );
                assertTrue( entity.properties()[1].toString().startsWith( i + "-" ) );

                assertTrue( entity.lineNumber() > previousLineNumber );
                previousLineNumber = entity.lineNumber();

                assertTrue( entity.position() > previousPosition );
                previousPosition = entity.position();
            }
            assertFalse( deserializer.hasNext() );
            assertEquals( threads, observedProcessingThreads.size() );
        }
    }

    private Data<InputNode> testData( int entities )
    {
        StringBuilder string = new StringBuilder();
        string.append( ":ID,name\n" );
        for ( int i = 0; i < entities; i++ )
        {
            string.append( i ).append( "," ).append( i ).append( "-" ).append( random.string() ).append( "\n" );
        }
        return data( string.toString() );
    }

    private Data<InputNode> data( String string )
    {
        return new Data<InputNode>()
        {
            @Override
            public CharReadable stream()
            {
                return wrap( new StringReader( string ) );
            }

            @Override
            public Function<InputNode,InputNode> decorator()
            {
                return item -> item;
            }
        };
    }
}
