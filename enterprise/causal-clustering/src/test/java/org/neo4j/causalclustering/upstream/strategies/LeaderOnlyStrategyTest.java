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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;


public class LeaderOnlyStrategyTest
{
    @Test
    public void ignoresSelf() throws UpstreamDatabaseSelectionException
    {
        // given
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );
        String groupName = "groupName";

        // and
        LeaderOnlyStrategy leaderOnlyStrategy = new LeaderOnlyStrategy();
        TopologyServiceThatPrioritisesItself topologyServiceNoRetriesStrategy = new TopologyServiceThatPrioritisesItself( myself, groupName )
        {
            @Override
            public Map<MemberId,RoleInfo> allCoreRoles()
            {
                Map<MemberId,RoleInfo> roles = new HashMap<>();
                roles.put( myself, RoleInfo.LEADER );
                roles.put( coreNotSelf, RoleInfo.LEADER );
                return roles;
            }
        };
        leaderOnlyStrategy.inject( topologyServiceNoRetriesStrategy, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> resolved = leaderOnlyStrategy.upstreamDatabase();

        // then
        Assert.assertTrue( resolved.isPresent() );
        Assert.assertNotEquals( myself, resolved.get() );
    }
}
