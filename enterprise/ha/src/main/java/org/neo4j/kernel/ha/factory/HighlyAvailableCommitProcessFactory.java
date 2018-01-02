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
package org.neo4j.kernel.ha.factory;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.CommitProcessSwitcher;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.lang.reflect.Proxy.newProxyInstance;

class HighlyAvailableCommitProcessFactory implements CommitProcessFactory
{
    private final LifeSupport modeSwitchersLife;
    private final Master master;
    private final TransactionPropagator transactionPropagator;
    private final RequestContextFactory requestContextFactory;
    private final HighAvailabilityModeSwitcher highAvailabilityModeSwitcher;

    private final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate =
            new DelegateInvocationHandler<>( TransactionCommitProcess.class );

    HighlyAvailableCommitProcessFactory( LifeSupport modeSwitchersLife, Master master,
            TransactionPropagator transactionPropagator, RequestContextFactory requestContextFactory,
            HighAvailabilityModeSwitcher highAvailabilityModeSwitcher )
    {
        this.modeSwitchersLife = modeSwitchersLife;
        this.master = master;
        this.transactionPropagator = transactionPropagator;
        this.requestContextFactory = requestContextFactory;
        this.highAvailabilityModeSwitcher = highAvailabilityModeSwitcher;
    }

    @Override
    public TransactionCommitProcess create( TransactionAppender appender,
            TransactionRepresentationStoreApplier storeApplier, NeoStoreInjectedTransactionValidator txValidator,
            IndexUpdatesValidator indexUpdatesValidator, Config config )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTransactionCommitProcess();
        }

        removeOldCommitSwitcher();

        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess( appender, storeApplier,
                indexUpdatesValidator );

        CommitProcessSwitcher commitProcessSwitcher = new CommitProcessSwitcher( transactionPropagator,
                master, commitProcessDelegate, requestContextFactory, highAvailabilityModeSwitcher,
                txValidator, commitProcess );

        modeSwitchersLife.add( commitProcessSwitcher );

        return (TransactionCommitProcess) newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
    }

    /**
     * {@link CommitProcessSwitcher} register itself as listener of {@link HighAvailabilityModeSwitcher} during
     * {@link CommitProcessSwitcher#start()} and de-registers in {@link CommitProcessSwitcher#stop()}.
     * Problem is that life that contains {@link CommitProcessSwitcher} is only started on the database startup and
     * stopped during it's shutdown. That is why we need to manually stop old switcher and remove it from the life
     * when new commit process is create.
     */
    private void removeOldCommitSwitcher()
    {
        for ( Lifecycle instance : modeSwitchersLife.getLifecycleInstances() )
        {
            if ( instance instanceof CommitProcessSwitcher )
            {
                modeSwitchersLife.remove( instance );
            }
        }
    }
}
