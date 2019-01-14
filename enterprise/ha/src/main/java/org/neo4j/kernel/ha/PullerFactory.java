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
package org.neo4j.kernel.ha;

import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

/**
 * Helper factory that provide more convenient way of construction and dependency management for update pulling
 * related components
 */
public class PullerFactory
{
    private final RequestContextFactory requestContextFactory;
    private final Master master;
    private final LastUpdateTime lastUpdateTime;
    private final LogProvider logging;
    private final InstanceId serverId;
    private final InvalidEpochExceptionHandler invalidEpochHandler;
    private final long pullInterval;
    private final JobScheduler jobScheduler;
    private final DependencyResolver dependencyResolver;
    private final AvailabilityGuard availabilityGuard;
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final Monitors monitors;

    public PullerFactory( RequestContextFactory requestContextFactory, Master master,
            LastUpdateTime lastUpdateTime, LogProvider logging, InstanceId serverId,
            InvalidEpochExceptionHandler invalidEpochHandler, long pullInterval,
            JobScheduler jobScheduler, DependencyResolver dependencyResolver, AvailabilityGuard availabilityGuard,
            HighAvailabilityMemberStateMachine memberStateMachine, Monitors monitors )
    {

        this.requestContextFactory = requestContextFactory;
        this.master = master;
        this.lastUpdateTime = lastUpdateTime;
        this.logging = logging;
        this.serverId = serverId;
        this.invalidEpochHandler = invalidEpochHandler;
        this.pullInterval = pullInterval;
        this.jobScheduler = jobScheduler;
        this.dependencyResolver = dependencyResolver;
        this.availabilityGuard = availabilityGuard;
        this.memberStateMachine = memberStateMachine;
        this.monitors = monitors;
    }

    public SlaveUpdatePuller createSlaveUpdatePuller()
    {
        return new SlaveUpdatePuller( requestContextFactory, master, lastUpdateTime, logging, serverId,
                availabilityGuard, invalidEpochHandler, jobScheduler,
                monitors.newMonitor( SlaveUpdatePuller.Monitor.class ) );
    }

    public UpdatePullingTransactionObligationFulfiller createObligationFulfiller( UpdatePuller updatePuller )
    {
        return new UpdatePullingTransactionObligationFulfiller( updatePuller, memberStateMachine, serverId,
                dependencyResolver.provideDependency( TransactionIdStore.class ) );
    }

    public UpdatePullerScheduler createUpdatePullerScheduler( UpdatePuller updatePuller )
    {
        return new UpdatePullerScheduler( jobScheduler, logging, updatePuller, pullInterval );
    }
}
