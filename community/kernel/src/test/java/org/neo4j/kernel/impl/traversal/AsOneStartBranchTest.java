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
package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.kernel.Uniqueness;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsOneStartBranchTest
{
    @Test
    public void donNotExhaustIteratorWhenUsingRelationshipPath()
    {
        // Given
        Iterable<Node> nodeIterable = mock( Iterable.class );
        Iterator<Node> nodeIterator = mock( Iterator.class );
        when(nodeIterable.iterator()).thenReturn( nodeIterator );
        when( nodeIterator.hasNext() ).thenReturn( true );

        // When
        new AsOneStartBranch( mock( TraversalContext.class ), nodeIterable, mock( InitialBranchState.class ), Uniqueness.RELATIONSHIP_PATH );

        // Then
        verify( nodeIterator, never() ).next();
    }

}
