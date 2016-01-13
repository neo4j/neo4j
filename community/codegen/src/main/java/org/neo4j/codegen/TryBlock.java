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
package org.neo4j.codegen;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * TryBlock are not really block but tries to look like it from an API standpoint.
 * Keeps a state-machine to figure out which state we're in (try, catch, finally).
 * It is not until we call the last "try block" we actually call close on the actual
 * code block and generates code for the entire try-catch block at once.
 */
public class TryBlock extends CodeBlock
{
    private final Resource[] resources;
    private final State tryState = newState( null );
    private final List<CatchState> catchStates = new LinkedList<>();
    private State finallyState = null;

    private State currentState = tryState;

    TryBlock( CodeBlock parent, Resource... resources )
    {
        super( parent );
        this.resources = resources;
    }

    public CodeBlock catchBlock(Parameter exception)
    {
        CatchState catchState = new CatchState( exception, currentState );
        this.currentState = catchState;
        catchStates.add( catchState );
        return this;
    }

    public CodeBlock finallyBlock()
    {
        if ( finallyState != null )
        {
            throw new IllegalStateException( "Cannot have more than one finally block" );
        }
        finallyState = newState( currentState );
        currentState = finallyState;
        return this;
    }

    @Override
    protected void emit( Consumer<MethodEmitter> emitFunction )
    {
        currentState.addAction( emitFunction );
    }

    @Override
    public void close()
    {
        State nextState = currentState.nextState();
        if ( nextState == null )
        {
            super.emit( ( e ) -> e.tryCatchBlock( tryActions(), catchClauses(), finallyActions(), localVariables, resources ) );
            super.close();
        }
        currentState = nextState;

    }

    @Override
    protected void endBlock()
    {
        //do nothing
    }

    private List<Consumer<MethodEmitter>> tryActions()
    {
        return tryState.actions();
    }

    private List<CatchClause> catchClauses()
    {
        return catchStates.stream().map( c -> new CatchClause( c.exception, c.actions) )
                .collect( Collectors.toList() );
    }

    private List<Consumer<MethodEmitter>> finallyActions()
    {
        return finallyState == null ? Collections.emptyList() : finallyState.actions();
    }

    /**
     * A Try block can be in different states depending on if we're in try, catch or finally.
     * When nextState returns <tt>null</tt> it means we have no more blocks left and should close
     * the underlying code block.
     */
    private interface State
    {

        void addAction( Consumer<MethodEmitter> action );

        List<Consumer<MethodEmitter>> actions();

        State nextState();
    }

    /**
     * Catch states are special states that stores what kind of exception we are catching.
     */
    private static class CatchState implements State
    {

        private final List<Consumer<MethodEmitter>> actions = new LinkedList<>();
        private final Parameter exception;
        private final State parent;

        CatchState( Parameter exception, State parent )
        {
            this.exception = exception;
            this.parent = parent;
        }

        @Override
        public void addAction( Consumer<MethodEmitter> action )
        {
            actions.add( action );
        }

        @Override
        public List<Consumer<MethodEmitter>> actions()
        {
            return actions;
        }

        @Override
        public State nextState()
        {
            return parent;
        }
    }
    private State newState( State parent )
    {
        return new State()
        {
            private final List<Consumer<MethodEmitter>> actions = new LinkedList<>();

            @Override
            public void addAction( Consumer<MethodEmitter> action )
            {
                actions.add( action );
            }

            @Override
            public List<Consumer<MethodEmitter>> actions()
            {
                return actions;
            }

            @Override
            public State nextState()
            {
                return parent;
            }
        };
    }
}
