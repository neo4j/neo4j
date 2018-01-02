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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;

/**
 * Factory for initial state of {@link TraversalBranch}es in a traversal.
 *
 * @param <STATE> type of initial state to produce.
 * 
 * @deprecated use {@link InitialBranchState} instead, which has got
 * {@link InitialBranchState#reverse()} as well.
 */
public interface InitialStateFactory<STATE>
{
    /**
     * An {@link InitialStateFactory} which returns {@code null} as state.
     */
    @SuppressWarnings( "rawtypes" )
    InitialStateFactory NO_STATE = new InitialStateFactory()
    {
        @Override
        public Object initialState( Path path )
        {
            return null;
        }
    };
    
    /**
     * Returns an initial state for a {@link Path}. All paths entering this method
     * are start paths(es) of a traversal. State is passed down along traversal
     * branches as the traversal progresses and can be changed at any point by a
     * {@link PathExpander} to becomes the new state from that point in that branch
     * and downwards.
     * 
     * @param path the start branch to return the initial state for.
     * @return an initial state for the traversal branch.
     */
    STATE initialState( Path path );

    /**
     * Wraps an {@link InitialStateFactory} in a {@link InitialBranchState}
     */
    class AsInitialBranchState<STATE> implements InitialBranchState<STATE>
    {
        private final InitialStateFactory<STATE> factory;

        public AsInitialBranchState( InitialStateFactory<STATE> factory )
        {
            this.factory = factory;
        }

        @Override
        public InitialBranchState<STATE> reverse()
        {
            return this;
        }

        @Override
        public STATE initialState( Path path )
        {
            return factory.initialState( path );
        }
    }
}
