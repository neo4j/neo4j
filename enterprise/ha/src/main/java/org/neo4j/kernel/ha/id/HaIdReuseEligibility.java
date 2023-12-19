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
package org.neo4j.kernel.ha.id;

import java.time.Clock;

import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;

/**
 * This {@link IdReuseEligibility} only buffer ids for reuse when we're the master.
 * This is mostly an optimization since when in slave role the ids are thrown away anyway.
 */
public class HaIdReuseEligibility implements IdReuseEligibility
{
    private final ClusterMembers members;
    private final Clock clock;
    private final long idReuseSafeZone;

    public HaIdReuseEligibility( ClusterMembers members, Clock clock, long idReuseSafeZone )
    {
        this.members = members;
        this.clock = clock;
        this.idReuseSafeZone = idReuseSafeZone;
    }

    @Override
    public boolean isEligible( KernelTransactionsSnapshot snapshot )
    {
        switch ( members.getCurrentMemberRole() )
        {
        case HighAvailabilityModeSwitcher.SLAVE:
            // If we're slave right now then just release them because the id generators in slave mode
            // will throw them away anyway, no need to keep them in memory. The architecture around
            // how buffering is done isn't a 100% fit for HA since the wrapping if IdGeneratorFactory
            // where the buffering takes place is done in a place which is oblivious to HA and roles
            // which means that buffering will always take place. For now we'll have to live with
            // always buffering and only just release them as soon as possible when slave.
            return true;
        case HighAvailabilityModeSwitcher.MASTER:
            // If we're master then we have to keep these ids around during the configured safe zone time
            // so that slaves have a chance to read consistently as well (slaves will know and compensate
            // for falling outside of safe zone).
            return clock.millis() - snapshot.snapshotTime() >= idReuseSafeZone;
        default:
            // If we're anything other than slave, i.e. also pending then retain the ids since we're
            // not quite sure what state we're in at the moment and we clear the id buffers anyway
            // during state switch.
            return false;
        }
    }
}
