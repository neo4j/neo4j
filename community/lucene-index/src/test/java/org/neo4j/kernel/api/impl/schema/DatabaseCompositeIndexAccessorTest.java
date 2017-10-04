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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.IOFunction;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.schema.IndexQuery.exact;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@RunWith( Parameterized.class )
public class DatabaseCompositeIndexAccessorTest
{
    private static final int PROP_ID1 = 1;
    private static final int PROP_ID2 = 2;
    private static final IndexDescriptor DESCRIPTOR = IndexDescriptorFactory.forLabel( 0, PROP_ID1, PROP_ID2 );
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @ClassRule
    public static final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Parameterized.Parameter
    public IOFunction<DirectoryFactory,LuceneIndexAccessor> accessorFactory;

    private LuceneIndexAccessor accessor;
    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object[] values = {"value1", "values2"};
    private final Object[] values2 = {40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexDescriptor indexDescriptor = IndexDescriptorFactory
            .forLabel( 0, PROP_ID1, PROP_ID2 );
    private static final IndexDescriptor uniqueIndexDescriptor = IndexDescriptorFactory
            .uniqueForLabel( 1, PROP_ID1, PROP_ID2 );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<IOFunction<DirectoryFactory,LuceneIndexAccessor>[]> implementations()
    {
        final File dir = new File( "dir" );
        return Arrays.asList(
                arg( dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( indexDescriptor )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "1" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, DESCRIPTOR );
                } ),
                arg( dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( uniqueIndexDescriptor )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "testIndex" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, DESCRIPTOR );
                } )
        );
    }

    @SuppressWarnings( "unchecked" )
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
    public void after() throws IOException
    {
        accessor.close();
        dirFactory.close();
    }

    @Test
    public void indexReaderShouldSupportScan() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        PrimitiveLongIterator results = reader.query( IndexQuery.exists( PROP_ID1 ), IndexQuery.exists( PROP_ID2 ) );

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), PrimitiveLongCollections.toSet( results ) );
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, values ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, values2 ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( firstReader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        assertEquals( asSet(), PrimitiveLongCollections.toSet( firstReader
                .query( exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) ) );
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( secondReader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        assertEquals( asSet( nodeId2 ), PrimitiveLongCollections.toSet( secondReader
                .query( exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        reader.close();
    }

    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ) ) );

        // WHEN
        updateAndCommit( asList( change( nodeId, values, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) ) );
        assertEquals( emptySet(), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, values ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) ) );
        assertEquals( asSet(), PrimitiveLongCollections.toSet( reader
                .query( exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) ) );
        reader.close();
    }

    @Test
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // when
        IndexReader indexReader = accessor.newReader(); // needs to be acquired before drop() is called
        IndexSampler indexSampler = indexReader.createSampler();

        Future<Void> drop = threading.executeAndAwait( (IOFunction<Void,Void>) nothing ->
        {
            accessor.drop();
            return nothing;
        }, null, waitingWhileIn( TaskCoordinator.class, "awaitCompletion" ), 3, SECONDS );

        try ( IndexReader reader = indexReader /* do not inline! */ )
        {
            indexSampler.sampleIndex();
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

    private IndexEntryUpdate<?> add( long nodeId, Object... values )
    {
        return IndexQueryHelper.add( nodeId, indexDescriptor.schema(), values );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object... values )
    {
        return IndexQueryHelper.remove( nodeId, indexDescriptor.schema(), values );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object[] valuesBefore, Object[] valuesAfter )
    {
        return IndexQueryHelper.change( nodeId, indexDescriptor.schema(), valuesBefore, valuesAfter );
    }

    private void updateAndCommit( List<IndexEntryUpdate<?>> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate<?> update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
