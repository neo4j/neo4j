/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.lang.StackWalker.StackFrame;
import java.util.Optional;
import java.util.function.Predicate;

public class StackTraceElementGuardedAdversary implements Adversary
{
    private final Adversary delegate;
    private final Predicate<StackFrame> check;
    private volatile boolean enabled;

    StackTraceElementGuardedAdversary( Adversary delegate, Predicate<StackFrame> check )
    {
        this.delegate = delegate;
        this.check = check;
        enabled = true;
    }

    @Override
    public void injectFailure( Class<? extends Throwable>... failureTypes )
    {
        if ( enabled && calledFromVictimStackTraceElement() )
        {
            delegate.injectFailure( failureTypes );
        }
    }

    @Override
    public boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
    {
        return enabled && calledFromVictimStackTraceElement() && delegate.injectFailureOrMischief( failureTypes );
    }

    @Override
    public Optional<Throwable> getLastAdversaryException()
    {
        return delegate.getLastAdversaryException();
    }

    private boolean calledFromVictimStackTraceElement()
    {
        return StackWalker.getInstance().walk( s -> s.filter( check ).findAny() ).isPresent();
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
