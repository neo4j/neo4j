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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

class StartNodeTraversalBranch extends TraversalBranchWithState
{
    private final InitialBranchState initialState;
    
    StartNodeTraversalBranch( TraversalContext context, TraversalBranch parent, Node source,
            InitialBranchState initialState )
    {
        super( parent, source, initialState );
        this.initialState = initialState;
        evaluate( context );
        context.isUniqueFirst( this );
    }

    @Override
    public TraversalBranch next( PathExpander expander, TraversalContext metadata )
    {
        if ( !hasExpandedRelationships() )
        {
            expandRelationships( expander );
            return this;
        }
        return super.next( expander, metadata );
    }
    
    @Override
    protected TraversalBranch newNextBranch( Node node, Relationship relationship )
    {
        return initialState != InitialBranchState.NO_STATE ?
            new TraversalBranchWithState( this, 1, node, relationship, stateForChildren ) :
            new TraversalBranchImpl( this, 1, node, relationship );
    }
}
