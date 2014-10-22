/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Arrays;

import org.junit.Test;

import org.neo4j.kernel.impl.store.counts.CountsKey;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.store.counts.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.relationshipKey;

public class CountsKeyTest
{
    @Test
    public void shouldSortNodeKeysBeforeRelationshipKeys() throws Exception
    {
        // given
        CountsKey[] array = {
                relationshipKey( 13, 2, 21 ),
                relationshipKey( 17, 2, 21 ),
                relationshipKey( 21, 1, 13 ),
                nodeKey( 13 ), nodeKey( 17 ), nodeKey( 21 ),
        };

        // when
        Arrays.sort( array );

        // then
        assertEquals( Arrays.asList( nodeKey( 13 ), nodeKey( 17 ), nodeKey( 21 ),
                                     relationshipKey( 21, 1, 13 ),
                                     relationshipKey( 13, 2, 21 ),
                                     relationshipKey( 17, 2, 21 ) ),
                      Arrays.asList( array ) );
    }
}
