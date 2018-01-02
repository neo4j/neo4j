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

/**
 * Accessor for a state associated with a {@link TraversalBranch} during a
 * traversal. A {@link TraversalBranch} can have an associated state which
 * follows down the branch as the traversal goes. If the state is modified
 * with {@link #setState(Object)} it means that branches further down
 * will have the newly set state, until it potentially gets overridden
 * again. The state returned from {@link #getState()} represents the state
 * associated with the parent branch, which by this point has followed down
 * to the branch calling {@link #getState()}.
 * 
 * @author Mattias Persson
 *
 * @param <STATE> the type of object the state is.
 */
public interface BranchState<STATE>
{
    /**
     * @return the associated state for a {@link TraversalBranch}.
     */
    STATE getState();
    
    /**
     * Sets the {@link TraversalBranch} state for upcoming children of that
     * branch.
     * @param state the {@link TraversalBranch} state to set for upcoming
     * children.
     */
    void setState( STATE state );

    /**
     * Instance representing no state, usage resulting in
     * {@link IllegalStateException} being thrown.
     */
    BranchState NO_STATE = new BranchState()
    {
        @Override
        public Object getState()
        {
            throw new IllegalStateException( "Branch state disabled, pass in an initial state to enable it" );
        }

        @Override
        public void setState( Object state )
        {
            throw new IllegalStateException( "Branch state disabled, pass in an initial state to enable it" );
        }
    };
}
