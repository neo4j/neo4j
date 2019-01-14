/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class LeaderOnlyStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "leader-only";

    public LeaderOnlyStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        Map<MemberId,RoleInfo> memberRoles = topologyService.allCoreRoles();

        if ( memberRoles.size() == 0 )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        for ( Map.Entry<MemberId,RoleInfo> entry : memberRoles.entrySet() )
        {
            RoleInfo role = entry.getValue();
            if ( role == RoleInfo.LEADER && !Objects.equals( myself, entry.getKey() ) )
            {
                return Optional.of( entry.getKey() );
            }
        }

        return Optional.empty();
    }
}
