/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.set;

public class DiscoveredMemberRepositoryTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    @Test
    public void shouldStoreDiscoveredMembers() throws Exception
    {
        // given
        DiscoveredMemberRepository discoveredMemberRepositoryA =
                new DiscoveredMemberRepository( testDirectory.directory(), fileSystem, NullLogProvider.getInstance() );

        Set<AdvertisedSocketAddress> members =
                set( new AdvertisedSocketAddress( "localhost:5003" ), new AdvertisedSocketAddress( "localhost:5004" ),
                        new AdvertisedSocketAddress( "localhost:5005" ) );

        discoveredMemberRepositoryA.store( members );

        // when
        DiscoveredMemberRepository discoveredMemberRepositoryB =
                new DiscoveredMemberRepository( testDirectory.directory(), fileSystem, NullLogProvider.getInstance() );

        // then
        assertEquals(members, discoveredMemberRepositoryB.previouslyDiscoveredMembers() );
    }
}
