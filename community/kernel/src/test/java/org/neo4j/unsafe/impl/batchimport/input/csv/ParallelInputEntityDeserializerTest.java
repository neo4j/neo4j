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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutionPanicException;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static java.lang.String.format;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.kernel.impl.util.Validators.emptyValidator;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DeserializerFactories.defaultNodeDeserializer;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.ACTUAL;

public class ParallelInputEntityDeserializerTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldParseDataInParallel() throws Exception
    {
        // GIVEN
        int entities = 500;
        Data<InputNode> data = testData( entities );
        Configuration config = new Configuration.Overridden( COMMAS )
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
        DeserializerFactory<InputNode> deserializerFactory = ( header, chunk, decorator, validator ) ->
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
                    new InputNodeDeserialization( header, chunk, groups, idType.idsAreExternal() ), decorator,
                    validator, badCollector );
        };
        try ( ParallelInputEntityDeserializer<InputNode> deserializer = new ParallelInputEntityDeserializer<>( data,
                defaultFormatNodeFileHeader(), config, idType, threads, threads, deserializerFactory,
                Validators.emptyValidator(), InputNode.class ) )
        {
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

    // Timeout is so that if this bug strikes again it will only cause this test to run for a limited time
    // before failing. Normally this test is really quick
    @Test( timeout = 10_000 )
    public void shouldTreatExternalCloseAsPanic() throws Exception
    {
        // GIVEN enough data to fill up queues
        int entities = 500;
        Data<InputNode> data = testData( entities );
        Configuration config = new Configuration.Overridden( COMMAS )
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

        // WHEN closing before having consumed all results
        DeserializerFactory<InputNode> deserializerFactory =
                defaultNodeDeserializer( groups, config, idType, badCollector );
        boolean noticedPanic = false;
        try ( ParallelInputEntityDeserializer<InputNode> deserializer = new ParallelInputEntityDeserializer<>( data,
                defaultFormatNodeFileHeader(), config, idType, 3, 3, deserializerFactory,
                Validators.emptyValidator(), InputNode.class ) )
        {
            deserializer.hasNext();
            RuntimeException panic = new RuntimeException();
            deserializer.receivePanic( panic );

            // Why pull some items after it has been closed? The above close() symbolizes a panic from
            // somewhere, anywhere in the importer. At that point there are still batches that have been
            // processed and are there for the taking. One of the components in the hang scenario that we want
            // to test comes from a processor in TicketedProcessing forever trying to offer its processed
            // result to the result queue (where the loop didn't care if it had been forcefully shut down.
            // To get one of the processing threads into doing that we need to pull some of the already
            // processed items so that it wants to go ahead and offer its result.
            try
            {
                for ( int i = 0; i < 100 && deserializer.hasNext(); i++ )
                {
                    deserializer.next();
                }
            }
            catch ( RuntimeException e )
            {
                // THEN it should notice as it goes through the results
                assertSame( panic, e );
                noticedPanic = true;
            }
        }
        catch ( TaskExecutionPanicException e )
        {
            // THEN it should be able to exit (this exception comes as a side effect if iteration above didn't see it)
            noticedPanic = true;
        }
        assertTrue( noticedPanic );
    }

    @Test
    public void shouldParseInputGroupWithSeparateHeader() throws Exception
    {
        // given
        Data<InputNode> data = nodesWithSeparateHeader( 20, 3 );
        try ( InputIterator<InputNode> deserializer = new ParallelInputEntityDeserializer<>( data, defaultFormatNodeFileHeader(),
                COMMAS, ACTUAL, 1, 1, defaultNodeDeserializer( new Groups(), COMMAS, ACTUAL, Collector.EMPTY ),
                emptyValidator(), InputNode.class ) )
        {
            // then
            assertEquals( 20 * 3, count( deserializer ) );
        }
    }

    private static Data<InputNode> nodesWithSeparateHeader( int entriesPerFile, int files )
    {
        List<CharReadable> sources = new ArrayList<>();
        sources.add( Readables.wrap( ":ID" ) );
        int id = 0;
        for ( int f = 0; f < files; f++ )
        {
            StringBuilder builder = new StringBuilder();
            for ( int i = 0; i < entriesPerFile; i++ )
            {
                builder.append( id++ );
                builder.append( format( "%n" ) );
            }
            sources.add( Readables.wrap( builder.toString() ) );
        }

        return new Data<InputNode>()
        {
            @Override
            public RawIterator<CharReadable,IOException> stream()
            {
                return Readables.iterator( a -> a, sources.toArray( new CharReadable[sources.size()] ) );
            }

            @Override
            public Decorator<InputNode> decorator()
            {
                return InputEntityDecorators.NO_NODE_DECORATOR;
            }
        };
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

    private static Data<InputNode> data( String string )
    {
        return DataFactories.data( InputEntityDecorators.NO_NODE_DECORATOR, () -> wrap( string ) ).create( COMMAS );
    }
}
