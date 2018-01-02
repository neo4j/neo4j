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
package org.neo4j.adversaries;

import org.neo4j.function.Predicate;

/**
 * An adversary that delegates failure injection only when invoked through certain call sites.
 * For every potential failure injection the current stack trace (the elements of it) are analyzed
 * and if there's a match with the specified victims then failure will be delegated to the actual
 * {@link Adversary} underneath.
 */
public class ClassGuardedAdversary implements Adversary
{
    private final Adversary delegate;
    private final Predicate<StackTraceElement>[] victimFilters;
    private volatile boolean enabled;

    /**
     * Specifies victims as class names
     *
     * @param delegate {@link Adversary} to delegate calls to.
     * @param victimClassNames fully qualified names of classes which will provoke failures.
     */
    public ClassGuardedAdversary( Adversary delegate, String... victimClassNames )
    {
        this( delegate, toVictims( victimClassNames ) );
    }

    /**
     * Specifies victims as arbitrary {@link StackTraceElement} {@link Predicate}.
     *
     * @param delegate {@link Adversary} to delegate calls to.
     * @param victims arbitrary {@link Predicate} for {@link StackTraceElement} in the executing
     * thread and if any of the elements in the current stack trace matches then failure is injected.
     */
    @SafeVarargs
    public ClassGuardedAdversary( Adversary delegate, Predicate<StackTraceElement>... victims )
    {
        this.delegate = delegate;
        victimFilters = victims;
        enabled = true;
    }

    @SuppressWarnings( {"unchecked", "deprecation"} )
    private static Predicate<StackTraceElement>[] toVictims( String[] victimClassNames )
    {
        Predicate[] victims = new Predicate[victimClassNames.length];
        for ( int i = 0; i < victimClassNames.length; i++ )
        {
            final String victimClassName = victimClassNames[i];
            victims[i] = new Predicate<StackTraceElement>()
            {
                @Override
                public boolean test( StackTraceElement element )
                {
                    return element.getClassName().equals( victimClassName );
                }
            };
        }
        return victims;
    }

    @Override
    public void injectFailure( Class<? extends Throwable>... failureTypes )
    {
        if ( enabled && calledFromVictimClass() )
        {
            delegateFailureInjection( failureTypes );
        }
    }

    @Override
    public boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
    {
        if ( enabled && calledFromVictimClass() )
        {
            return delegateFailureOrMischiefInjection( failureTypes );
        }
        return false;
    }

    protected void delegateFailureInjection( Class<? extends Throwable>[] failureTypes )
    {
        delegate.injectFailure( failureTypes );
    }

    protected boolean delegateFailureOrMischiefInjection( Class<? extends Throwable>[] failureTypes )
    {
        return delegate.injectFailureOrMischief( failureTypes );
    }

    private boolean calledFromVictimClass()
    {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for ( StackTraceElement element : stackTrace )
        {
            for ( Predicate<StackTraceElement> victimFilter : victimFilters )
            {
                if ( victimFilter.test( element ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    public void disable()
    {
        enabled = false;
    }

    public void enable()
    {
        enabled = true;
    }
}
