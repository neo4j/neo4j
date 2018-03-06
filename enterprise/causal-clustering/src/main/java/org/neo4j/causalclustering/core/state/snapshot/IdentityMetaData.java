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
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class IdentityMetaData
{
    private final AdvertisedSocketAddress address;
    private final ClusterId clusterId;
    private final StoreId storeId;
    private final MemberId memberId;
    private final Role role;

    public IdentityMetaData( AdvertisedSocketAddress address, ClusterId clusterId, StoreId storeId, MemberId memberId, Role role )
    {
        this.address = address;
        this.clusterId = clusterId;
        this.memberId = memberId;
        this.role = role;
        this.storeId = storeId;
    }

    public AdvertisedSocketAddress getAddress()
    {
        return address;
    }

    public ClusterId getClusterId()
    {
        return clusterId;
    }

    public MemberId getMemberId()
    {
        return memberId;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    public Role getRole()
    {
        return role;
    }

    @Override
    public String toString()
    {
        return String.format( "%s{address=%s,role=%s,cluster=%s}", memberId, address, role, clusterId );
    }
}
