/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.storage;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.*;

public class SimpleStorageTest
{
    @Rule
    public EphemeralFileSystemRule fsa = new EphemeralFileSystemRule();

    @Test
    public void shouldWriteAndReadState() throws Exception
    {
        // given
        SimpleStorage<MemberId> storage = new SimpleFileStorage<>( fsa.get(), new File( "state-dir" ), "member-id-a", new MemberId.Marshal(), NullLogProvider.getInstance() );

        // when
        MemberId idA = new MemberId( UUID.randomUUID() );
        storage.writeState( idA );
        MemberId idB = storage.readState();

        // then
        assertEquals( idA.getUuid(), idB.getUuid() );
        assertEquals( idA, idB );
    }
}
