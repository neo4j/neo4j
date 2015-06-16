/*
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
package org.neo4j.kernel.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public abstract class IndexAccessorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    protected IndexAccessor accessor;
    private static final int PROPERTY_KEY_ID = 100;

    private boolean isUnique = true;

    public IndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, boolean isUnique )
    {
        super(testSuite);
        this.isUnique = isUnique;
    }

    @Before
    public void before() throws Exception
    {
        IndexConfiguration indexConfig = new IndexConfiguration( isUnique );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexConfig, indexSamplingConfig );
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
    public void testIndexPrefixSearch() throws Exception
    {
        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, "A", new long[]{1000} ),
                NodePropertyUpdate.add( 3L, PROPERTY_KEY_ID, "apa", new long[]{1000} ),
                NodePropertyUpdate.add( 4L, PROPERTY_KEY_ID, "apA", new long[]{1000} ),
                NodePropertyUpdate.add( 5L, PROPERTY_KEY_ID, "b", new long[]{1000} ) ) );

        assertThat( getAllNodesFromPrefixSearch( "a" ), equalTo( asList( 1L, 3L, 4L ) ) );
        assertThat( getAllNodesFromPrefixSearch( "A" ), equalTo( asList( 2L ) ) );
        assertThat( getAllNodesFromPrefixSearch( "ba" ), equalTo( Collections.EMPTY_LIST ) );
        assertThat( getAllNodesFromPrefixSearch( "" ), equalTo( asList( 1L, 2L, 3L, 4L, 5L ) ) );
    }

    @Test
    public void testIndexPrefixSearchOnNonStrings() throws Exception
    {
        updateAndCommit( asList(
                NodePropertyUpdate.add( 1L, PROPERTY_KEY_ID, "a", new long[]{1000} ),
                NodePropertyUpdate.add( 2L, PROPERTY_KEY_ID, 2L, new long[]{1000} ) ) );
        assertThat( getAllNodesFromPrefixSearch( "2" ), equalTo( Collections.EMPTY_LIST ) );
    }

    protected List<Long> getAllNodesWithProperty( String propertyValue ) throws IOException
    {
        try ( IndexReader reader = accessor.newReader() )
        {
            List<Long> list = new LinkedList<>();
            for ( PrimitiveLongIterator iterator = reader.lookup( propertyValue ); iterator.hasNext(); )
            {
                list.add( iterator.next() );
            }
            Collections.sort( list );
            return list;
        }
    }

    protected List<Long> getAllNodesFromPrefixSearch( String prefix ) throws IOException
    {
        try ( IndexReader reader = accessor.newReader() )
        {
            List<Long> list = new LinkedList<>();
            for ( PrimitiveLongIterator iterator = reader.lookupByPrefixSearch( prefix ); iterator.hasNext(); )
            {
                list.add( iterator.next() );
            }
            Collections.sort( list );
            return list;
        }
    }

    protected List<Long> getAllNodes() throws IOException
    {
        try ( IndexReader reader = accessor.newReader() )
        {
            List<Long> list = new LinkedList<>();
            for ( PrimitiveLongIterator iterator = reader.scan(); iterator.hasNext(); )
            {
                list.add( iterator.next() );
            }
            Collections.sort( list );
            return list;
        }
    }

    protected void updateAndCommit( List<NodePropertyUpdate> updates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : updates )
            {
                updater.process( update );
            }
        }
    }

}
