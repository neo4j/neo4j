/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * An adversary that delegates failure injection only when invoked through certain call sites.
 * For every potential failure injection the current stack trace (the elements of it) are analyzed
 * and if there's a match with the specified victims then failure will be delegated to the actual
 * {@link Adversary} underneath.
 */
public class ClassGuardedAdversary extends StackTraceElementGuardedAdversary
{

    public ClassGuardedAdversary( Adversary delegate, Class<?>... victimClassSet )
    {
        super( delegate, new Predicate<StackTraceElement>()
        {
            private final Set<String> victimClasses = Stream.of( victimClassSet ).map( Class::getName ).collect( toSet() );

            @Override
            public boolean test( StackTraceElement stackTraceElement )
            {
                return victimClasses.contains( stackTraceElement.getClassName() );
            }
        } );
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
        super( delegate, victims );
    }
}
