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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RelationshipHolesTest
{
    @Test
    public void shouldFindEdgeOfHole() throws Exception
    {
        // GIVEN
        // 10->20->4->15->3
        RelationshipHoles holes = new RelationshipHoles();
        long node = 0;

        // WHEN
        // delete 4 and 15
        holes.deleted( relationship( 4, node, 15 ) );
        holes.deleted( relationship( 15, node, 3 ) );

        // THEN
        assertTrue( holes.accept( 4 ) );
        assertTrue( holes.accept( 15 ) );
        assertFalse( holes.accept( 20 ) );

        assertEquals( 3L, holes.apply( node, 4 ) );
        assertEquals( 3L, holes.apply( node, 15 ) );
        assertEquals( 20L, holes.apply( node, 20 ) );
    }

    private RelationshipRecord relationship( long id, long node, long next )
    {
        return new RelationshipRecord( id, false, node, IGNORE, 0, IGNORE, next, IGNORE, IGNORE, false, false );
    }

    private static final int IGNORE = -99;
}
