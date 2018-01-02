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
package org.neo4j.function;

/**
 * Constructors for basic {@link IntPredicate} types
 * @deprecated This class relies on deprecated interfaces, and will be retrofitted to work with the {@code java.util.function} interfaces in 3.0.
 */
public final class IntPredicates
{
    private static final IntPredicate TRUE = new IntPredicate()
    {
        @Override
        public boolean test( int value )
        {
            return true;
        }
    };

    private static final IntPredicate FALSE = new IntPredicate()
    {
        @Override
        public boolean test( int value )
        {
            return false;
        }
    };

    /**
     * A predicate that is always true
     *
     * @return true
     */
    public static IntPredicate alwaysTrue()
    {
        return TRUE;
    }

    /**
     * A predicate that is always false
     *
     * @return false
     */
    public static IntPredicate alwaysFalse()
    {
        return FALSE;
    }
}
