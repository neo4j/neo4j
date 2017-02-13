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
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
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

public abstract class IndexAccessorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    protected IndexAccessor accessor;

    private boolean isUnique = true;

    public IndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, boolean isUnique )
    {
        super(testSuite);
        this.isUnique = isUnique;
    }

    @Before
    public void before() throws Exception
    {
        IndexConfiguration indexConfig = IndexConfiguration.of( isUnique );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
        IndexPopulator populator = indexProvider.getPopulator( 17, IndexBoundary.map( descriptor ), indexConfig, indexSamplingConfig );
        populator.create();
        populator.close( true );
        accessor = indexProvider.getOnlineAccessor( 17, indexConfig, indexSamplingConfig );
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
                IndexEntryUpdate.add( 1L, descriptor, -5 ),
                IndexEntryUpdate.add( 2L, descriptor, 0 ),
                IndexEntryUpdate.add( 3L, descriptor, 5.5 ),
                IndexEntryUpdate.add( 4L, descriptor, 10.0 ),
                IndexEntryUpdate.add( 5L, descriptor, 100.0 ) ) );

        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( 0, 10 ), equalTo( asList( 2L, 3L, 4L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( 10, null ), equalTo( asList( 4L, 5L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( 100, 0 ), equalTo( EMPTY_LIST ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( null, 5.5 ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( null, null ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -5, 0 ), equalTo( asList( 1L, 2L ) ) );
        assertThat( getAllNodesFromInclusiveIndexSeekByNumber( -5, 5.5 ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

    @Test
    public void testIndexSeekByString() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor, "Anabelle" ),
                IndexEntryUpdate.add( 2L, descriptor, "Anna" ),
                IndexEntryUpdate.add( 3L, descriptor, "Bob" ),
                IndexEntryUpdate.add( 4L, descriptor, "Harriet" ),
                IndexEntryUpdate.add( 5L, descriptor, "William" ) ) );

        assertThat( getAllNodesFromIndexSeekByString( "Anna", true, "Harriet", false ), equalTo( asList( 2L, 3L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Harriet", true, null, false ), equalTo( asList( 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Harriet", false, null, true ), equalTo( singletonList( 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "William", false, "Anna", true ), equalTo( EMPTY_LIST ) );
        assertThat( getAllNodesFromIndexSeekByString( null, false, "Bob", false ), equalTo( asList( 1L, 2L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( null, true, "Bob", true ), equalTo( asList( 1L, 2L, 3L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( null, true, null, true ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Anabelle", false, "Anna", true ), equalTo( singletonList( 2L ) ) );
        assertThat( getAllNodesFromIndexSeekByString( "Anabelle", false, "Bob", false ), equalTo( singletonList( 2L ) ) );
    }

    @Test
    public void testIndexSeekByPrefix() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor, "a" ),
                IndexEntryUpdate.add( 2L, descriptor, "A" ),
                IndexEntryUpdate.add( 3L, descriptor, "apa" ),
                IndexEntryUpdate.add( 4L, descriptor, "apA" ),
                IndexEntryUpdate.add( 5L, descriptor, "b" ) ) );

        assertThat( getAllNodesFromIndexSeekByPrefix( "a" ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "A" ), equalTo( Collections.singletonList( 2L ) ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "ba" ), equalTo( EMPTY_LIST ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "" ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexSeekByPrefixOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                IndexEntryUpdate.add( 1L, descriptor, "a" ),
                IndexEntryUpdate.add( 2L, descriptor, 2L ) ) );
        assertThat( getAllNodesFromIndexSeekByPrefix( "2" ), equalTo( EMPTY_LIST ) );
    }

    protected List<Long> getAllNodesWithProperty( String propertyValue ) throws IOException
    {
        return metaGet( reader -> reader.seek( propertyValue ));
    }

    protected List<Long> getAllNodesFromInclusiveIndexSeekByNumber( Number lower, Number upper ) throws IOException
    {
        return metaGet( reader -> reader.rangeSeekByNumberInclusive( lower, upper ));
    }

    protected List<Long> getAllNodesFromIndexSeekByString( String lower, boolean includeLower, String upper, boolean includeUpper ) throws IOException
    {
        return metaGet( reader -> reader.rangeSeekByString( lower, includeLower, upper, includeUpper ));
    }

    protected List<Long> getAllNodesFromIndexSeekByPrefix( String prefix ) throws IOException
    {
        return metaGet( reader -> reader.rangeSeekByPrefix( prefix));
    }

    protected List<Long> getAllNodesFromIndexScanByContains( String term ) throws IOException
    {
        return metaGet( reader -> reader.containsString( term ) );
    }

    protected List<Long> getAllNodesFromIndexScanEndsWith( String term ) throws IOException
    {
        return metaGet( reader -> reader.endsWith( term ) );
    }

    protected List<Long> getAllNodes() throws IOException
    {
        return metaGet( IndexReader::scan );
    }

    private List<Long> metaGet( ReaderInteraction interaction )
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
        PrimitiveLongIterator results( IndexReader reader );
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
