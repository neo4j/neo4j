/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test;

import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;

public class NonUniqueIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public NonUniqueIndexPopulatorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    @Test
    public void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception
    {
        // when
        IndexPopulator populator = indexProvider.getPopulator( 17, new IndexConfiguration( false ) );
        populator.create();
        populator.add( 1, "value1" );
        populator.add( 2, "value1" );
        populator.close( true );

        // then
        IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, new IndexConfiguration( false ) );
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator nodes = reader.lookup( "value1" );
        assertEquals( asSet( 1l, 2l ), asSet( nodes ) );
        reader.close();
        accessor.close();
    }
    
    @Test
    public void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception
    {
        // GIVEN
        IndexPopulator populator = indexProvider.getPopulator( 17, new IndexConfiguration( false ) );
        String failure = "The contrived failure";
        
        // WHEN
        populator.markAsFailed( failure );
        
        // THEN
        assertEquals( failure, indexProvider.getPopulationFailure( 17 ) );
    }
    
    @Test
    public void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception
    {
        // GIVEN
        IndexPopulator populator = indexProvider.getPopulator( 17, new IndexConfiguration( false ) );
        String failure = "The contrived failure";
        
        // WHEN
        populator.markAsFailed( failure );
        
        // THEN
        assertEquals( FAILED, indexProvider.getInitialState( 17 ) );
    }
    
    @Test
    public void shouldBeAbleToDropAClosedIndexPopulator() throws Exception
    {
        // GIVEN
        IndexPopulator populator = indexProvider.getPopulator( 17, new IndexConfiguration( false ) );
        populator.close( false );
        
        // WHEN
        populator.drop();
        
        // THEN - no exception should be thrown (it's been known to!)
    }
}
