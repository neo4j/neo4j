/*
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
package org.neo4j.kernel.ha.factory;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;

import static java.lang.reflect.Proxy.newProxyInstance;

class HighlyAvailableCommitProcessFactory implements CommitProcessFactory
{
    private final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate;

    HighlyAvailableCommitProcessFactory( DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate )
    {
        this.commitProcessDelegate = commitProcessDelegate;
    }

    @Override
    public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine,
            Config config )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTransactionCommitProcess();
        }
        return (TransactionCommitProcess) newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
    }
}
