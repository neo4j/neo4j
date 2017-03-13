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
package org.neo4j.kernel.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.schema_new.IndexQuery;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.schema_new.IndexQuery.range;

public abstract class IndexAccessorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    protected IndexAccessor accessor;

    public IndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite,
            NewIndexDescriptor descriptor, boolean isUnique )
    {
        super( testSuite, descriptor );
    }

    @Before
    public void before() throws Exception
    {
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexSamplingConfig );
        populator.create();
        populator.close( true );
        accessor = indexProvider.getOnlineAccessor( 17, descriptor, indexSamplingConfig );
    }

    @After
    public void after() throws IOException
    {
        accessor.drop();
        accessor.close();
    }

    @Test
    public void testIndexSeekByNumber() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), -5 ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), 0 ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), 5.5 ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), 10.0 ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), 100.0 ) ) );

        assertThat( query( range( 1, 0, true, 10, true ) ), equalTo( asList( 2L, 3L, 4L ) ) );
        assertThat( query( range( 1, 10, true, null, true ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 1, 100, true, 0, true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 1, null, true, 5.5, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 1, (Number)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 1, -5, true, 0, true ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 1, -5, true, 5.5, true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexSeekByString() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), "Anabelle" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), "Anna" ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), "Bob" ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), "Harriet" ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), "William" ) ) );

        assertThat( query( range( 1, "Anna", true, "Harriet", false ) ), equalTo( asList( 2L, 3L ) ) );
        assertThat( query( range( 1, "Harriet", true, null, false ) ), equalTo( asList( 4L, 5L ) ) );
        assertThat( query( range( 1, "Harriet", false, null, true ) ), equalTo( singletonList( 5L ) ) );
        assertThat( query( range( 1, "William", false, "Anna", true ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( range( 1, null, false, "Bob", false ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( range( 1, null, true, "Bob", true ) ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( query( range( 1, (String)null, true, null, true ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( query( range( 1, "Anabelle", false, "Anna", true ) ), equalTo( singletonList( 2L ) ) );
        assertThat( query( range( 1, "Anabelle", false, "Bob", false ) ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekByPrefix() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), "A" ),
                IndexEntryUpdate.add( 3L, descriptor.schema(), "apa" ),
                IndexEntryUpdate.add( 4L, descriptor.schema(), "apA" ),
                IndexEntryUpdate.add( 5L, descriptor.schema(), "b" ) ) );

        assertThat( query( IndexQuery.stringPrefix( 1, "a" ) ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "A" ) ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "ba" ) ), equalTo( EMPTY_LIST ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "" ) ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexSeekByPrefixOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor.schema(), "a" ),
                IndexEntryUpdate.add( 2L, descriptor.schema(), 2L ) ) );
        assertThat( query( IndexQuery.stringPrefix( 1, "2" ) ), equalTo( EMPTY_LIST ) );
    }

    protected List<Long> query( IndexQuery... predicates ) throws Exception
    {
        return metaGet( reader -> reader.query( predicates ) );
    }

    private List<Long> metaGet( ReaderInteraction interaction ) throws Exception
    {
        try ( IndexReader reader = accessor.newReader() )
        {
            List<Long> list = new LinkedList<>();
            for ( PrimitiveLongIterator iterator = interaction.results( reader ); iterator.hasNext(); )
            {
                list.add( iterator.next() );
            }
            Collections.sort( list );
            return list;
        }
    }

    private interface ReaderInteraction
    {
        PrimitiveLongIterator results( IndexReader reader ) throws Exception;
    }

    protected void updateAndCommit( List<IndexEntryUpdate> updates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate update : updates )
            {
                updater.process( update );
            }
        }
    }

}
