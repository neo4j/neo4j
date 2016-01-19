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
import java.util.Deque;
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
    private final State tryState = State.empty();
    private final List<CatchState> catchStates = new LinkedList<>();
    private State finallyState = null;
    private State currentState = tryState;

    TryBlock( CodeBlock parent, Resource... resources )
    {
        super( parent );
        for ( Resource resource : resources )
        {
            localVariables.createNew( resource.type(), resource.name() );
        }
        this.resources = resources;
    }

    public CodeBlock catchBlock(Parameter exception)
    {
        CatchState catchState = new CatchState( exception, currentState );
        localVariables.createNew( exception.type(), exception.name() );
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
        finallyState = new State(currentState);
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
        currentState.close();
        State nextState = currentState.nextState();
        if ( nextState == null )
        {
            createTryCatchFinallyBlock();
            super.close();
        }
        currentState = nextState;
    }

    protected void createTryCatchFinallyBlock()
    {
        super.emit( ( e ) -> e.tryCatchBlock( tryActions(), catchClauses(), finallyActions(),
                localVariables, resources ) );
    }

    @Override
    public CodeBlock ifStatement( Expression test )
    {
        emit( e -> e.beginIf( test ) );
        currentState.innerBlock( () -> emit( MethodEmitter::endBlock ) );
        return this;
    }

    @Override
    public CodeBlock whileLoop( Expression test )
    {
        emit( e -> e.beginWhile( test ) );
        currentState.innerBlock( () -> emit( MethodEmitter::endBlock ) );
        return this;
    }

    @Override
    public TryBlock tryBlock( Resource... resources )
    {
        return new NestedTryBlock( this, resources );
    }

    @Override
    protected void endBlock()
    {
        //do nothing
    }

    protected List<Consumer<MethodEmitter>> tryActions()
    {
        return tryState.actions();
    }

    protected List<CatchClause> catchClauses()
    {
        return catchStates.stream().map( c -> new CatchClause( c.exception, c.actions()) )
                .collect( Collectors.toList() );
    }

    protected List<Consumer<MethodEmitter>> finallyActions()
    {
        return finallyState == null ? Collections.emptyList() : finallyState.actions();
    }

    private static class State
    {
        private final Deque<State> stack = new LinkedList<>();
        private final Deque<Runnable> closeActions = new LinkedList<>();
        static State empty()
        {
            return new State(null);
        }

        static State from(State parent)
        {
            return new State(parent);
        }

        private State( State parent )
        {
            if ( parent != null )
            {
                stack.push( parent );
            }
        }

        private final List<Consumer<MethodEmitter>> actions = new LinkedList<>();

        public void addAction( Consumer<MethodEmitter> action )
        {
            actions.add( action );
        }

        public List<Consumer<MethodEmitter>> actions()
        {
            return actions;
        }

        public State nextState()
        {
            return stack.isEmpty() ? null : stack.pop();
        }

        public void innerBlock( Runnable... onCloseActions)
        {
            stack.push( this );
            for ( Runnable action : onCloseActions )
            {
                closeActions.push(action);
            }
        }

        public void close()
        {
            if ( !closeActions.isEmpty() )
            {
                closeActions.pop().run();
            }
          }
    }

    /**
     * Catch states are special states that stores what kind of exception we are catching.
     */
    private static class CatchState extends State
    {

        private final Parameter exception;

        CatchState( Parameter exception, State parent )
        {
            super(parent);
            this.exception = exception;
        }
    }

    private class NestedTryBlock extends TryBlock {

        NestedTryBlock( CodeBlock parent, Resource... resources )
        {
            super( parent, resources );
        }

//        @Override
//        protected void emit( Consumer<MethodEmitter> emitFunction )
//        {
//            currentState.addAction( emitFunction );
//        }

        @Override
        protected void createTryCatchFinallyBlock()
        {
            currentState.addAction( ( e ) -> e.tryCatchBlock( tryActions(), catchClauses(), finallyActions(),
                    localVariables, resources ) );
        }
    }
}
