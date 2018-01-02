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
package org.neo4j.kernel.ha;

import org.neo4j.cluster.InstanceId;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
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

    public UpdatePuller createUpdatePuller( LifeSupport life )
    {
        return life.add( new SlaveUpdatePuller( requestContextFactory, master, lastUpdateTime, logging, serverId,
                availabilityGuard, invalidEpochHandler, jobScheduler,
                monitors.newMonitor( SlaveUpdatePuller.Monitor.class ) ) );
    }

    public TransactionObligationFulfiller createObligationFulfiller( LifeSupport life, UpdatePuller updatePuller )
    {
        return life.add( new UpdatePullingTransactionObligationFulfiller( updatePuller, memberStateMachine, serverId,
                dependencyResolver.provideDependency( TransactionIdStore.class ) ) );
    }

    public UpdatePullerScheduler createUpdatePullerScheduler( UpdatePuller updatePuller )
    {
        return new UpdatePullerScheduler( jobScheduler, logging, updatePuller, pullInterval );
    }
}
