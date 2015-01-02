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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.util.StringLogger;

public class TxHookModeSwitcher extends AbstractModeSwitcher<RemoteTxHook>
{
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactoryResolver requestContextFactory;
    private final StringLogger log;
    private final DependencyResolver resolver;

    public TxHookModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                               DelegateInvocationHandler<RemoteTxHook> delegate,
                               DelegateInvocationHandler<Master> master,
                               RequestContextFactoryResolver requestContextFactory, StringLogger log,
                               DependencyResolver resolver )
    {
        super( stateMachine, delegate );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.log = log;
        this.resolver = resolver;
    }

    @Override
    protected RemoteTxHook getMasterImpl()
    {
        return new MasterTxHook();
    }

    @Override
    protected RemoteTxHook getSlaveImpl( URI serverHaUri )
    {
        return new SlaveTxHook( master.cement(), resolver.resolveDependency( HaXaDataSourceManager.class ),
                requestContextFactory, log );
    }

    public interface RequestContextFactoryResolver
    {
        RequestContextFactory get();
    }
}
