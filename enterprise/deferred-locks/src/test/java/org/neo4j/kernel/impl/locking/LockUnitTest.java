/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, 1, false );

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
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, 1, true );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP, 2, true );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit3, unit2, unit5, unit4 ), list );
    }
}
