/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Test;

import java.util.Collections;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;

public class TraversalBranchImplTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldExpandOnFirstAccess() throws Exception
    {
        // GIVEN
        TraversalBranch parent = mock( TraversalBranch.class );
        Node source = mock( Node.class );
        TraversalBranchImpl branch = new TraversalBranchImpl( parent, source );
        @SuppressWarnings( "rawtypes" )
        PathExpander expander = mock( PathExpander.class );
        when( expander.expand( eq( branch ), any( BranchState.class ) ) ).thenReturn( Collections.emptySet() );
        TraversalContext context = mock( TraversalContext.class );
        when( context.evaluate( eq( branch ), any( BranchState.class ) ) ).thenReturn( INCLUDE_AND_CONTINUE );

        // WHEN initializing
        branch.initialize( expander, context );

        // THEN the branch should not be expanded
        verifyZeroInteractions( source );

        // and WHEN actually traversing from it
        branch.next( expander, context );

        // THEN we should expand it
        verify( expander ).expand( any( Path.class ), any( BranchState.class ) );
    }
}
