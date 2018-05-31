/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.util.Preconditions.requirePositive;

class PreconditionsTest
{

    @Test
    void requirePositiveOk()
    {
        requirePositive( 1 );
    }

    @Test
    void requirePositiveFailsOnZero()
    {
        assertThrows( IllegalArgumentException.class, () -> Preconditions.requirePositive( 0 ) );
    }

    @Test
    void requirePositiveFailsOnNegative()
    {
        assertThrows( IllegalArgumentException.class, () -> requirePositive( -1 ) );
    }

    @Test
    void requireNonNegativeOk()
    {
        Preconditions.requireNonNegative( 0 );
        Preconditions.requireNonNegative( 1 );
    }

    @Test
    void requireNonNegativeFailsOnNegative()
    {
        assertThrows( IllegalArgumentException.class, () -> Preconditions.requireNonNegative( -1 ) );
    }

    @Test
    void checkStateOk()
    {
        Preconditions.checkState( true, "must not fail" );
    }

    @Test
    void checkStateFails()
    {
        assertThrows( IllegalStateException.class, () -> Preconditions.checkState( false, "must fail" ) );
    }
}
