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
package org.neo4j.kernel.impl.api;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.neo4j.kernel.api.StatementContext;

public class CachingStatementContextTest
{
    @Test
    public void shouldGetCachedLabelsIfCached()
    {
        // GIVEN
        long nodeId = 3;
        Set<Long> labels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        PersistenceCache cache = mock( PersistenceCache.class );
        when( cache.getLabels( nodeId ) ).thenReturn( labels );
        StatementContext actual = mock( StatementContext.class );
        StatementContext context = new CachingStatementContext( actual, cache, null );
        
        // WHEN
        Iterator<Long> receivedLabels = context.getLabelsForNode( nodeId );
        
        // THEN
        assertEquals( labels, addToCollection(receivedLabels, new HashSet<Long>() ) );
    }
    
    @Test
    public void shouldAnswerAddLabelItselfIfNodeAlreadyHasLabel() throws Exception
    {
        // GIVEN
        long nodeId = 3;
        Set<Long> labels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        PersistenceCache cache = mock( PersistenceCache.class );
        when( cache.getLabels( nodeId ) ).thenReturn( labels );
        StatementContext actual = mock( StatementContext.class );
        StatementContext context = new CachingStatementContext( actual, cache, null );

        // WHEN
        boolean added = context.addLabelToNode( 2L, nodeId );

        // THEN
        assertFalse( added );
        verifyZeroInteractions( actual );
    }

    @Test
    public void shouldAnswerRemoveLabelItselfIfNodeAlreadyHasLabel() throws Exception
    {
        // GIVEN
        long nodeId = 3;
        Set<Long> labels = new HashSet<Long>( asList( 1L, 3L ) );
        PersistenceCache cache = mock( PersistenceCache.class );
        when( cache.getLabels( nodeId ) ).thenReturn( labels );
        StatementContext actual = mock( StatementContext.class );
        StatementContext context = new CachingStatementContext( actual, cache, null );

        // WHEN
        boolean removed = context.removeLabelFromNode( 2L, nodeId );

        // THEN
        assertFalse( removed );
        verifyZeroInteractions( actual );
    }
}
