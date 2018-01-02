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
package org.neo4j.test.randomized;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Randomized tester, where it's given a target factory, i.e. a factory for creating the object
 * participating in all actions. I.e. the subject of this randomized test. It's also given an action factory,
 * i.e. a factory for the actions to execute on the target.
 *
 * An {@link Action} can report if there an error happened, and if so the test halts indicating the order
 * of the action that failed. From there a the {@link #findMinimalReproducible() minimal reproducible list of actions}
 * can be figured out and {@link #testCaseWriter(String, Printable) test case} can be written.
 *
 * @param <T> type of target for the actions, i.e. the subject under test here.
 * @param <V> type of value the actions revolve around.
 * @param <F> type of failure an {@link Action} may produce.
 */
public class RandomizedTester<T extends TestResource,F>
{
    private final List<Action<T,F>> givenActions = new ArrayList<>();
    private final TargetFactory<T> targetFactory; // will be used later for finding a minimal test case
    private final ActionFactory<T,F> actionFactory;
    private Action<T,F> failingAction;
    private F failure;

    public RandomizedTester( TargetFactory<T> targetFactory, ActionFactory<T,F> actionFactory )
    {
        this( targetFactory, actionFactory, null, null );
    }

    private RandomizedTester( TargetFactory<T> targetFactory, ActionFactory<T,F> actionFactory,
            Action<T,F> failingAction, F failure )
    {
        this.targetFactory = targetFactory;
        this.actionFactory = actionFactory;
        this.failingAction = failingAction;
        this.failure = failure;
    }

    /**
     * @return the index of the failing action, or -1 if successful.
     */
    public Result<T,F> run( int numberOfActions )
    {
        try ( T target = targetFactory.newInstance() )
        {
            for ( int i = 0; i < numberOfActions; i++ )
            {
                // Create a new action
                Action<T,F> action = actionFactory.apply( target );

                // Have the effects of it applied to the target
                F failure = action.apply( target );
                if ( failure != null )
                {   // Something went wrong.
                    this.failingAction = action;
                    this.failure = failure;
                    return new Result<>( target, i, failure );
                }

                // Add it to the list of actions performed
                givenActions.add( action );
            }

            // Full verification
            return new Result<>( target, -1, failure );
        }
    }

    /**
     * Starts with the existing list of actions that were produced by {@link #run(int)}, trying to prune actions
     * from that list, while still being able to reproduce the exact same failure. The result is a new
     * {@link RandomizedTester} instance. with a potentially reduced list of actions.
     * @return a reduced list of actions to reproduce the failure.
     */
    public RandomizedTester<T,F> findMinimalReproducible()
    {
        RandomizedTester<T,F> minimal = this;
        while ( true )
        {
            RandomizedTester<T,F> candidate = minimal.reduceOneAction();
            if ( candidate == minimal )
            {
                return candidate;
            }
            minimal = candidate;
        }
    }

    private RandomizedTester<T,F> reduceOneAction()
    {
        int numberOfIterations = givenActions.size();
        if ( numberOfIterations == 1 )
        {
            return this;
        }
        for ( int actionToSkip = 0; actionToSkip < givenActions.size(); actionToSkip++ )
        {
            RandomizedTester<T,F> reducedActions = new RandomizedTester<>( targetFactory,
                    actionFactoryThatSkipsOneAction( givenActions.iterator(), actionToSkip, failingAction ),
                    failingAction, failure );
            Result<T,F> result = reducedActions.run( numberOfIterations-1 );
            if ( result.isFailure() && result.getIndex() == givenActions.size()-1 &&
                    result.getFailure().equals( failure ) )
            {
                return reducedActions;
            }
        }
        return this;
    }

    private ActionFactory<T,F> actionFactoryThatSkipsOneAction( final Iterator<Action<T,F>> iterator,
            final int actionToSkip, final Action<T,F> failingAction )
    {
        return new ActionFactory<T,F>()
        {
            private int index;
            private boolean failingActionReturned;

            @Override
            public Action<T,F> apply( T from )
            {
                if ( iterator.hasNext() )
                {
                    Action<T,F> action = iterator.next();
                    return index++ == actionToSkip ? apply( from ) : action;
                }

                if ( failingActionReturned )
                {
                    throw new IllegalStateException();
                }
                failingActionReturned = true;
                return failingAction;
            }
        };
    }

    public TestCaseWriter<T,F> testCaseWriter( String name, Printable given )
    {
        return new TestCaseWriter<>( name, given, targetFactory, givenActions, failingAction );
    }

    public F failure()
    {
        return failure;
    }

    public interface TargetFactory<T>
    {
        T newInstance();
    }

    public interface ActionFactory<T,F>
    {
        Action<T,F> apply( T from );
    }
}
