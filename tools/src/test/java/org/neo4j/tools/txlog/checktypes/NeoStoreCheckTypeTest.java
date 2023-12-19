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
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;

import static org.junit.Assert.assertTrue;

public class NeoStoreCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        NeoStoreRecord record1 = new NeoStoreRecord();
        record1.initialize( true, 1 );

        NeoStoreRecord record2 = record1.clone();

        NeoStoreCheckType check = new NeoStoreCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        NeoStoreRecord record1 = new NeoStoreRecord();
        record1.initialize( false, 1 );

        NeoStoreRecord record2 = new NeoStoreRecord();
        record2.initialize( false, 11 );

        NeoStoreCheckType check = new NeoStoreCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
