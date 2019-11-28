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
package org.neo4j.util;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdTest
{
    @Test
    void idsAreEqualIfSameUuid()
    {
        var uuid = UUID.randomUUID();

        var id1 = new Id( uuid );
        var id2 = new Id( uuid );

        assertEquals( id1, id2 );
    }

    @Test
    void idsAreNotEqualIfDifferentUuid()
    {
        var id1 = new Id( UUID.randomUUID() );
        var id2 = new Id( UUID.randomUUID() );

        assertNotEquals( id1, id2 );
    }

    @Test
    void shouldPrintShortName()
    {
        var uuid = UUID.randomUUID();
        var id = new Id( uuid );

        assertEquals( uuid.toString().substring( 0, 8 ), id.toString() );
    }

    @Test
    void shouldNotAllowNull()
    {
        var nullPointerException = assertThrows( NullPointerException.class, () -> new Id( null ) );

        assertEquals( nullPointerException.getMessage(), "UUID should be not null." );
    }
}
