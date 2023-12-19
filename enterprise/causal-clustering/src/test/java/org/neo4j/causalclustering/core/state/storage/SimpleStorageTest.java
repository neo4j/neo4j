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
package org.neo4j.causalclustering.core.state.storage;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class SimpleStorageTest
{
    @Rule
    public EphemeralFileSystemRule fsa = new EphemeralFileSystemRule();

    @Test
    public void shouldWriteAndReadState() throws Exception
    {
        // given
        SimpleStorage<MemberId> storage = new SimpleFileStorage<>( fsa.get(), new File( "state-dir" ),
                "member-id-a", new MemberId.Marshal(), NullLogProvider.getInstance() );

        // when
        MemberId idA = new MemberId( UUID.randomUUID() );
        storage.writeState( idA );
        MemberId idB = storage.readState();

        // then
        assertEquals( idA.getUuid(), idB.getUuid() );
        assertEquals( idA, idB );
    }
}
