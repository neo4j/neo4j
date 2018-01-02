/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LockUnitTest
{
    @Test
    public void exclusiveLocksAppearFirst()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, 1, true );
        LockUnit unit2 = new LockUnit( ResourceTypes.NODE, 2, false );
        LockUnit unit3 = new LockUnit( ResourceTypes.RELATIONSHIP, 1, false );
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP, 2, true );
        LockUnit unit5 = new LockUnit( ResourceTypes.SCHEMA, 1, false );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit4, unit2, unit3, unit5 ), list );
    }

    @Test
    public void exclusiveOrderedByResourceTypes()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, 1, true );
        LockUnit unit2 = new LockUnit( ResourceTypes.RELATIONSHIP, 1, true );
        LockUnit unit3 = new LockUnit( ResourceTypes.NODE, 2, true );
        LockUnit unit4 = new LockUnit( ResourceTypes.SCHEMA, 1, true );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP, 2, true );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit3, unit2, unit5, unit4 ), list );
    }
}
