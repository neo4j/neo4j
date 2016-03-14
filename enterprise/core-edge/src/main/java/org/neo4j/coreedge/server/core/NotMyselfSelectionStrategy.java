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
package org.neo4j.coreedge.server.core;

import java.util.Optional;

import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;

public class NotMyselfSelectionStrategy implements CoreServerSelectionStrategy
{
    private final CoreDiscoveryService discoveryService;
    private final CoreMember myself;

    public NotMyselfSelectionStrategy( CoreDiscoveryService discoveryService, CoreMember myself )
    {
        this.discoveryService = discoveryService;
        this.myself = myself;
    }

    @Override
    public AdvertisedSocketAddress coreServer() throws CoreServerSelectionException
    {
        Optional<CoreMember> member = discoveryService.currentTopology().getMembers().stream()
                .filter( coreMember -> !coreMember.equals( myself ) ).findFirst();

        if( member.isPresent() )
        {
            return member.get().getCoreAddress();
        }
        else
        {
            throw new CoreServerSelectionException( "No core servers available" );
        }
    }
}
