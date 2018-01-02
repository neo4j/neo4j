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
package org.neo4j.kernel.api.impl.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.IOFunction;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.register.Registers;
import org.neo4j.test.ThreadingRule;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.reserving;
import static org.neo4j.test.ThreadingRule.waitingWhileIn;

@RunWith( Parameterized.class )
public class LuceneIndexAccessorTest
{
    @Test
    public void indexReaderShouldSupportScan() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        PrimitiveLongIterator results = reader.scan();

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), asSet( results ) );
        assertEquals( asSet( nodeId ), asUniqueSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void indexReaderShouldHonorRepeatableReads() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, value2 ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.seek( value ) ) );
        assertEquals( asSet(  ), asUniqueSet( firstReader.seek( value2 ) ) );
        assertEquals( asSet( nodeId ), asUniqueSet( secondReader.seek( value ) ) );
        assertEquals( asSet( nodeId2 ), asUniqueSet( secondReader.seek( value2 ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );

        // WHEN
        updateAndCommit( asList( change( nodeId, value, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.seek( value2 ) ) );
        assertEquals( emptySetOf( Long.class ), asUniqueSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.seek( value2 ) ) );
        assertEquals( asSet(  ), asUniqueSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );

        // when
        IndexReader indexReader = accessor.newReader(); // needs to be acquired before drop() is called

        Future<Void> drop = threading.executeAndAwait( new IOFunction<Void, Void>()
        {
            @Override
            public Void apply( Void nothing ) throws IOException
            {
                accessor.drop();
                return nothing;
            }
        }, null, waitingWhileIn( TaskCoordinator.class, "awaitCompletion" ), 3, SECONDS );

        try ( IndexReader reader = indexReader /* do not inline! */ )
        {
            reader.sampleIndex( Registers.newDoubleLongRegister() );
            fail( "expected exception" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            assertEquals( "Index dropped while sampling.", e.getMessage() );
        }
        finally
        {
            drop.get();
        }
    }

    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value", value2 = 40;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    @Parameterized.Parameter
    public IOFunction<DirectoryFactory,LuceneIndexAccessor> accessorFactory;
    private LuceneIndexAccessor accessor;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<IOFunction<DirectoryFactory,LuceneIndexAccessor>[]> implementations()
    {
        final File dir = new File( "dir" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        return Arrays.asList(
                arg( new IOFunction<DirectoryFactory,LuceneIndexAccessor>()
                {
                    @Override
                    public LuceneIndexAccessor apply( DirectoryFactory dirFactory )
                            throws IOException
                    {
                        return new NonUniqueLuceneIndexAccessor( documentLogic, false, reserving(), dirFactory, dir,
                                100_000 );
                    }

                    @Override
                    public String toString()
                    {
                        return NonUniqueLuceneIndexAccessor.class.getName();
                    }
                } ),
                arg( new IOFunction<DirectoryFactory,LuceneIndexAccessor>()
                {
                    @Override
                    public LuceneIndexAccessor apply( DirectoryFactory dirFactory )
                            throws IOException
                    {
                        return new UniqueLuceneIndexAccessor( documentLogic, false, reserving(), dirFactory, dir );
                    }

                    @Override
                    public String toString()
                    {
                        return UniqueLuceneIndexAccessor.class.getName();
                    }
                } )
        );
    }

    private static IOFunction<DirectoryFactory,LuceneIndexAccessor>[] arg(
            IOFunction<DirectoryFactory,LuceneIndexAccessor> foo )
    {
        return new IOFunction[]{foo};
    }

    @Before
    public void before() throws IOException
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = accessorFactory.apply( dirFactory );
    }

    @After
    public void after()
    {
        dirFactory.close();
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate remove( long nodeId, Object value )
    {
        return NodePropertyUpdate.remove( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, 0, valueBefore, new long[0], valueAfter, new long[0] );
    }

    private void updateAndCommit( List<NodePropertyUpdate> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
