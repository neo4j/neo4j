/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An adversary that delegates failure injection only when invoked through certain classes.
 */
public class ClassGuardedAdversary implements Adversary
{
    private final Adversary delegate;
    private final Set<String> victimClasses;
    private volatile boolean enabled;

    public ClassGuardedAdversary( Adversary delegate, String... victimClassNames )
    {
        this.delegate = delegate;
        victimClasses = new HashSet<String>();
        Collections.addAll( victimClasses, victimClassNames );
        enabled = true;
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
            if ( victimClasses.contains( element.getClassName() ) )
            {
                return true;
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
