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

/**
 * Factory for initial state of {@link TraversalBranch}es in a traversal.
 *
 * @param <STATE> type of initial state to produce.
 */
public interface InitialBranchState<STATE> extends InitialStateFactory<STATE>
{
    @SuppressWarnings( "rawtypes" )
    InitialBranchState NO_STATE = new InitialBranchState()
    {
        @Override
        public Object initialState( Path path )
        {
            return null;
        }
        
        public InitialBranchState reverse()
        {
            return this;
        }
    };

    InitialBranchState<Double> DOUBLE_ZERO = new InitialBranchState()
    {
        @Override
        public Number initialState( Path path )
        {
            return 0d;
        }

        public InitialBranchState reverse()
        {
            return this;
        }
    };

    /**
     * Creates a version of this state factory which produces reversed initial state,
     * used in bidirectional traversals.
     * @return an instance which produces reversed initial state.
     */
    InitialBranchState<STATE> reverse();

    abstract class Adapter<STATE> implements InitialBranchState<STATE>
    {
        @Override
        public InitialBranchState<STATE> reverse()
        {
            return this;
        }
    }

    /**
     * Branch state evaluator for an initial state.
     */
    class State<STATE> extends Adapter<STATE>
    {
        private final STATE initialState;
        private final STATE reversedInitialState;

        public State( STATE initialState, STATE reversedInitialState )
        {
            this.initialState = initialState;
            this.reversedInitialState = reversedInitialState;
        }
        
        @Override
        public InitialBranchState<STATE> reverse()
        {
            return new State<STATE>( reversedInitialState, initialState );
        }

        @Override
        public STATE initialState( Path path )
        {
            return initialState;
        }
    }
}
