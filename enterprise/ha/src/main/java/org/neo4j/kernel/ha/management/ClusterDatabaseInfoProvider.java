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
package org.neo4j.kernel.ha.management;

import java.util.function.Function;

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
        if ( currentMember == null )
        {
            return null;
        }

        Function<Object,String> nullSafeToString = from -> from == null ? "" : from.toString();

        return new ClusterDatabaseInfo( new ClusterMemberInfo( currentMember.getInstanceId().toString(),
                currentMember.getHAUri() != null, true, currentMember.getHARole(),
                Iterables.asArray(String.class, Iterables.map( nullSafeToString, currentMember.getRoleURIs() ) ),
                Iterables.asArray(String.class, Iterables.map( nullSafeToString, currentMember.getRoles() ) ) ),
                txIdGetter.getLastTxId(), lastUpdateTime.getLastUpdateTime() );
    }
}
