/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;

import org.neo4j.kernel.impl.store.counts.CountsSnapshot;

import static org.junit.Assert.assertEquals;

public class LogRecoveryInfoTest
{
    LogRecoveryInfo logRecoveryInfo = new LogRecoveryInfo( new LogPosition( 42, 0 ), new CountsSnapshot( 42 ) );

    @Test
    public void testGetLogPosition() throws Exception
    {
        assertEquals( 42, logRecoveryInfo.getLogPosition().getLogVersion() );
    }

    @Test
    public void testGetCountsSnapshot() throws Exception
    {
        CountsSnapshot testSnapshot = new CountsSnapshot( 42 );
        assertEquals( testSnapshot.getTxId(), logRecoveryInfo.getCountsSnapshot().getTxId() );
    }
}