/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterTransactionCommitProcess;
import org.neo4j.kernel.ha.SlaveTransactionCommitProcess;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngine;

public class CommitProcessSwitcher extends AbstractComponentSwitcher<TransactionCommitProcess>
{
    private final TransactionPropagator txPropagator;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final DependencyResolver dependencyResolver;
    private final MasterTransactionCommitProcess.Monitor monitor;

    public CommitProcessSwitcher( TransactionPropagator txPropagator, Master master,
            DelegateInvocationHandler<TransactionCommitProcess> delegate, RequestContextFactory requestContextFactory,
            Monitors monitors, DependencyResolver dependencyResolver )
    {
        super( delegate );
        this.txPropagator = txPropagator;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.dependencyResolver = dependencyResolver;
        this.monitor = monitors.newMonitor( MasterTransactionCommitProcess.Monitor.class );
    }

    @Override
    protected TransactionCommitProcess getSlaveImpl()
    {
        return new SlaveTransactionCommitProcess( master, requestContextFactory );
    }

    @Override
    protected TransactionCommitProcess getMasterImpl()
    {
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                dependencyResolver.resolveDependency( TransactionAppender.class ),
                dependencyResolver.resolveDependency( StorageEngine.class ) );

        IntegrityValidator validator = dependencyResolver.resolveDependency( IntegrityValidator.class );
        return new MasterTransactionCommitProcess( commitProcess, txPropagator, validator, monitor );
    }
}
