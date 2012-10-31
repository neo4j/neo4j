/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.net.URI;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.StringLogger;

public class TxIdGeneratorModeSwitcher extends AbstractModeSwitcher<TxIdGenerator>
{
    private final HaXaDataSourceManager xaDsm;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private StringLogger msgLog;
    private Config config;
    private Slaves slaves;

    public TxIdGeneratorModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                                      DelegateInvocationHandler<TxIdGenerator> delegate, HaXaDataSourceManager xaDsm,
                                      Master master, RequestContextFactory requestContextFactory,
                                      StringLogger msgLog, Config config, Slaves slaves
    )
    {
        super( stateMachine, delegate );
        this.xaDsm = xaDsm;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.msgLog = msgLog;
        this.config = config;
        this.slaves = slaves;
    }

    @Override
    protected TxIdGenerator getMasterImpl()
    {
        return new MasterTxIdGenerator( MasterTxIdGenerator.from( config ), msgLog, slaves );
    }

    @Override
    protected TxIdGenerator getSlaveImpl( URI serverHaUri )
    {
        return new SlaveTxIdGenerator( config.get( HaSettings.server_id ), master,
                HighAvailabilityModeSwitcher.getServerId( serverHaUri ), requestContextFactory, xaDsm );
    }
}
