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
package org.neo4j.coreedge.messaging.routing;

import java.util.Optional;

import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.identity.MemberId;

public class NotMyselfSelectionStrategy implements CoreMemberSelectionStrategy
{
    private final CoreTopologyService discoveryService;
    private final MemberId myself;

    public NotMyselfSelectionStrategy( CoreTopologyService discoveryService, MemberId myself )
    {
        this.discoveryService = discoveryService;
        this.myself = myself;
    }

    @Override
    public MemberId coreMember() throws CoreMemberSelectionException
    {
        Optional<MemberId> member = discoveryService.currentTopology().coreMembers().stream()
                .filter( coreMember -> !coreMember.equals( myself ) ).findFirst();

        if ( member.isPresent() )
        {
            return member.get();
        }
        else
        {
            throw new CoreMemberSelectionException( "No core servers available" );
        }
    }
}
