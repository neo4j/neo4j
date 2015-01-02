/**
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

import org.neo4j.function.RawFunction;
import org.neo4j.helpers.TaskCoordinator;
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
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;
import static org.neo4j.test.ThreadingRule.stackTracePredicate;

@RunWith( Parameterized.class )
public class LuceneIndexAccessorTest
{
    @Test
    public void indexReaderShouldHonorRepeatableReads() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, value ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.lookup( value ) ) );
        assertEquals( asSet( nodeId, nodeId2 ), asUniqueSet( secondReader.lookup( value ) ) );
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
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
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
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value2 ) ) );
        assertEquals( emptySetOf( Long.class ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.lookup( value ) ) );
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

        Future<Void> drop = threading.executeAndAwait( new RawFunction<Void,Void,Exception>()
        {
            @Override
            public Void apply( Void nothing ) throws Exception
            {
                accessor.drop();
                return nothing;
            }
        }, null, stackTracePredicate( 3, TaskCoordinator.class, "awaitCompletion" ), 3, SECONDS );

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
    public RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException> accessorFactory;
    private LuceneIndexAccessor accessor;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException>[]> implementations()
    {
        final File dir = new File( "dir" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        final IndexWriterStatus writerLogic = new IndexWriterStatus();
        return Arrays.asList(
                arg( new RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException>()
                {
                    @Override
                    public LuceneIndexAccessor apply( DirectoryFactory dirFactory )
                            throws IOException
                    {
                        return new NonUniqueLuceneIndexAccessor( documentLogic, standard(), writerLogic, dirFactory,
                                dir,
                                100_000 );
                    }

                    @Override
                    public String toString()
                    {
                        return NonUniqueLuceneIndexAccessor.class.getName();
                    }
                } ),
                arg( new RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException>()
                {
                    @Override
                    public LuceneIndexAccessor apply( DirectoryFactory dirFactory )
                            throws IOException
                    {
                        return new UniqueLuceneIndexAccessor( documentLogic, standard(), writerLogic, dirFactory, dir );
                    }

                    @Override
                    public String toString()
                    {
                        return UniqueLuceneIndexAccessor.class.getName();
                    }
                } )
        );
    }

    private static RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException>[] arg(
            RawFunction<DirectoryFactory,LuceneIndexAccessor,IOException> foo )
    {
        return new RawFunction[]{foo};
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
            throws IOException, IndexEntryConflictException
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
