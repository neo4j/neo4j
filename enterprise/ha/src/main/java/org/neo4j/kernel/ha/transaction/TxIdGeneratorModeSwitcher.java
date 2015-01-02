/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.net.URI;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;

public class TxIdGeneratorModeSwitcher extends AbstractModeSwitcher<TxIdGenerator>
{
    private final HaXaDataSourceManager xaDsm;
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final StringLogger msgLog;
    private final Config config;
    private final Slaves slaves;
    private final AbstractTransactionManager tm;
    private final JobScheduler scheduler;

    public TxIdGeneratorModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                                      DelegateInvocationHandler<TxIdGenerator> delegate, HaXaDataSourceManager xaDsm,
                                      DelegateInvocationHandler<Master> master,
                                      RequestContextFactory requestContextFactory,
                                      StringLogger msgLog, Config config, Slaves slaves, AbstractTransactionManager tm,
                                      JobScheduler scheduler
    )
    {
        super( stateMachine, delegate );
        this.xaDsm = xaDsm;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.msgLog = msgLog;
        this.config = config;
        this.slaves = slaves;
        this.tm = tm;
        this.scheduler = scheduler;
    }

    @Override
    protected TxIdGenerator getMasterImpl()
    {
        return new MasterTxIdGenerator( MasterTxIdGenerator.from( config ), msgLog, slaves, new CommitPusher( scheduler ) );
    }

    @Override
    protected TxIdGenerator getSlaveImpl( URI serverHaUri )
    {
        return new SlaveTxIdGenerator( config.get( ClusterSettings.server_id ).toIntegerIndex(), master.cement(),
                HighAvailabilityModeSwitcher.getServerId( serverHaUri ).toIntegerIndex(), requestContextFactory, xaDsm,
                tm );
    }
}
