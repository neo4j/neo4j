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
package org.neo4j.legacy.consistency.checking.full;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiPassAvoidanceConditionTest
{
    @Test
    public void permitsOnlySinglePass()
    {
        MultiPassAvoidanceCondition<NodeRecord> condition = new MultiPassAvoidanceCondition<>();

        assertTrue( condition.test( new NodeRecord( 0 ) ) );
        assertTrue( condition.test( new NodeRecord( 1 ) ) );
        assertTrue( condition.test( new NodeRecord( 2 ) ) );

        assertFalse( condition.test( new NodeRecord( 0 ) ) );
        assertFalse( condition.test( new NodeRecord( 1 ) ) );
        assertFalse( condition.test( new NodeRecord( 2 ) ) );
    }

    /**
     * Checks the case when first record in the store is !inUse and is not delivered to the multi-pass check.
     */
    @Test
    public void permitsOnlySinglePassWhenFirstRecordDoesNotHaveIdZero()
    {
        MultiPassAvoidanceCondition<NodeRecord> condition = new MultiPassAvoidanceCondition<>();

        assertTrue( condition.test( new NodeRecord( 1 ) ) );
        assertTrue( condition.test( new NodeRecord( 2 ) ) );
        assertTrue( condition.test( new NodeRecord( 3 ) ) );

        assertFalse( condition.test( new NodeRecord( 1 ) ) );
        assertFalse( condition.test( new NodeRecord( 2 ) ) );
        assertFalse( condition.test( new NodeRecord( 3 ) ) );
    }
}
