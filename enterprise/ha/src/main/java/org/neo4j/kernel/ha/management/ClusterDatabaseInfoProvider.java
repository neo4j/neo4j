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
package org.neo4j.kernel.ha.management;

import org.neo4j.helpers.Functions;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

public class ClusterDatabaseInfoProvider
{
    private final ClusterMembers members;
    private final LastTxIdGetter txIdGetter;
    private final LastUpdateTime lastUpdateTime;

    public ClusterDatabaseInfoProvider( ClusterMembers members, LastTxIdGetter txIdGetter,
                                        LastUpdateTime lastUpdateTime )
    {
        this.members = members;
        this.txIdGetter = txIdGetter;
        this.lastUpdateTime = lastUpdateTime;
    }

    public ClusterDatabaseInfo getInfo()
    {
        ClusterMember currentMember = members.getCurrentMember();
        if (currentMember == null)
        {
            return null;
        }

        return new ClusterDatabaseInfo( new ClusterMemberInfo( currentMember.getInstanceId().toString(),
                currentMember.getHAUri() != null, true, currentMember.getHARole(),
                Iterables.toArray(String.class, Iterables.map( Functions.TO_STRING, currentMember.getRoleURIs() ) ),
                Iterables.toArray(String.class, Iterables.map( Functions.TO_STRING, currentMember.getRoles() ) ) ),
                txIdGetter.getLastTxId(), lastUpdateTime.getLastUpdateTime() );
    }
}
