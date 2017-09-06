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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.schema.IndexQuery.exact;
import static org.neo4j.kernel.api.schema.IndexQuery.range;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@RunWith( Parameterized.class )
public class DatabaseIndexAccessorTest
{
    public static final int PROP_ID = 1;

    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @ClassRule
    public static final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Parameterized.Parameter( 0 )
    public IndexDescriptor index;
    @Parameterized.Parameter( 1 )
    public IOFunction<DirectoryFactory,LuceneIndexAccessor> accessorFactory;

    private LuceneIndexAccessor accessor;
    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object value = "value";
    private final Object value2 = 40;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexDescriptor GENERAL_INDEX = IndexDescriptorFactory.forLabel( 0, PROP_ID );
    private static final IndexDescriptor UNIQUE_INDEX = IndexDescriptorFactory.uniqueForLabel( 1, PROP_ID );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> implementations()
    {
        final File dir = new File( "dir" );
        return Arrays.asList(
                arg( GENERAL_INDEX, dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( GENERAL_INDEX )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "1" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, GENERAL_INDEX );
                } ),
                arg( UNIQUE_INDEX, dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( UNIQUE_INDEX )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "testIndex" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, UNIQUE_INDEX );
                } )
        );
    }

    private static Object[] arg(
            IndexDescriptor index,
            IOFunction<DirectoryFactory,LuceneIndexAccessor> foo )
    {
        return new Object[]{index, foo};
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
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        PrimitiveLongIterator results = reader.query( IndexQuery.exists( PROP_ID ) );

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), PrimitiveLongCollections.toSet( results ) );
        assertEquals( asSet( nodeId ),
                PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value ) ) ) );
        reader.close();
    }

    @Test
    public void indexStringRangeQuery() throws Exception
    {
        updateAndCommit( asList( add( PROP_ID, "A" ), add( 2, "B" ), add( 3, "C" ), add( 4, "" ) ) );

        IndexReader reader = accessor.newReader();

        PrimitiveLongIterator rangeFromBInclusive = reader.query( range( PROP_ID, "B", true, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( rangeFromBInclusive ), LongArrayMatcher.of( 2, 3 ) );

        PrimitiveLongIterator rangeFromANonInclusive = reader.query( range( PROP_ID, "A", false, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( rangeFromANonInclusive ), LongArrayMatcher.of( 2, 3 ) );

        PrimitiveLongIterator emptyLowInclusive = reader.query( range( PROP_ID, "", true, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( emptyLowInclusive ), LongArrayMatcher.of( PROP_ID, 2, 3, 4 ) );

        PrimitiveLongIterator emptyUpperNonInclusive = reader.query( range( PROP_ID, "B", true, "", false ) );
        assertThat( PrimitiveLongCollections.asArray( emptyUpperNonInclusive ), LongArrayMatcher.emptyArrayMatcher() );

        PrimitiveLongIterator emptyInterval = reader.query( range( PROP_ID, "", true, "", true ) );
        assertThat( PrimitiveLongCollections.asArray( emptyInterval ), LongArrayMatcher.of( 4 ) );

        PrimitiveLongIterator emptyAllNonInclusive = reader.query( range( PROP_ID, "", false, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( emptyAllNonInclusive ), LongArrayMatcher.of( PROP_ID, 2, 3 ) );

        PrimitiveLongIterator nullNonInclusive = reader.query( range( PROP_ID, (String) null, false, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( nullNonInclusive ), LongArrayMatcher.of( PROP_ID, 2, 3, 4 ) );

        PrimitiveLongIterator nullInclusive = reader.query( range( PROP_ID, (String) null, false, null, false ) );
        assertThat( PrimitiveLongCollections.asArray( nullInclusive ), LongArrayMatcher.of( PROP_ID, 2, 3, 4 ) );
    }

    @Test
    public void indexNumberRangeQuery() throws Exception
    {
        updateAndCommit( asList( add( 1, 1 ), add( 2, 2 ), add( 3, 3 ), add( 4, 4 ), add( 5, Double.NaN ) ) );

        IndexReader reader = accessor.newReader();

        PrimitiveLongIterator rangeTwoThree = reader.query( range( PROP_ID, 2, true, 3, true ) );
        assertThat( PrimitiveLongCollections.asArray( rangeTwoThree ), LongArrayMatcher.of( 2, 3 ) );

        PrimitiveLongIterator infiniteMaxRange = reader.query( range( PROP_ID, 2, true, Long.MAX_VALUE, true ) );
        assertThat( PrimitiveLongCollections.asArray( infiniteMaxRange ), LongArrayMatcher.of( 2, 3, 4 ) );

        PrimitiveLongIterator infiniteMinRange = reader.query( range( PROP_ID, Long.MIN_VALUE, true, 3, true ) );
        assertThat( PrimitiveLongCollections.asArray( infiniteMinRange ), LongArrayMatcher.of( PROP_ID, 2, 3 ) );

        PrimitiveLongIterator maxNanInterval = reader.query( range( PROP_ID, 3, true, Double.NaN, true ) );
        assertThat( PrimitiveLongCollections.asArray( maxNanInterval ), LongArrayMatcher.of( 3, 4, 5 ) );

        PrimitiveLongIterator minNanInterval = reader.query( range( PROP_ID, Double.NaN, true, 5, true ) );
        assertThat( PrimitiveLongCollections.asArray( minNanInterval ), LongArrayMatcher.emptyArrayMatcher() );

        PrimitiveLongIterator nanInterval = reader.query( range( PROP_ID, Double.NaN, true, Double.NaN, true ) );
        assertThat( PrimitiveLongCollections.asArray( nanInterval ), LongArrayMatcher.of( 5 ) );
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
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value ) ) ) );
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
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( firstReader.query( exact( PROP_ID, value ) ) ) );
        assertEquals( asSet(), PrimitiveLongCollections.toSet( firstReader.query( exact( PROP_ID, value2 ) ) ) );
        assertEquals( asSet( nodeId ),
                PrimitiveLongCollections.toSet( secondReader.query( exact( PROP_ID, value ) ) ) );
        assertEquals( asSet( nodeId2 ),
                PrimitiveLongCollections.toSet( secondReader.query( exact( PROP_ID, value2 ) ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value ) ) ) );
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
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value2 ) ) ) );
        assertEquals( emptySet(),
                PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value ) ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value2 ) ) ) );
        assertEquals( asSet(), PrimitiveLongCollections.toSet( reader.query( exact( PROP_ID, value ) ) ) );
        reader.close();
    }

    @Test
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );

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

    private IndexEntryUpdate<?> add( long nodeId, Object value )
    {
        return IndexQueryHelper.add( nodeId, index.schema(), value );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object value )
    {
        return IndexQueryHelper.remove( nodeId, index.schema(), value );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return IndexQueryHelper.change( nodeId, index.schema(), valueBefore, valueAfter );
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
