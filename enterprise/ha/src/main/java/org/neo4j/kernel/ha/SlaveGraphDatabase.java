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

import java.util.Map;

import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Slave implementation of HA Graph Database
 */
public class SlaveGraphDatabase
    extends AbstractHAGraphDatabase
{
    private ResponseReceiver responseReceiver;
    private LastCommittedTxIdSetter lastCommittedTxIdSetter;
    private SlaveIdGenerator.SlaveIdGeneratorFactory slaveIdGeneratorFactory;

    public SlaveGraphDatabase( String storeDir, Map<String, String> params,
                               HighlyAvailableGraphDatabase highlyAvailableGraphDatabase,
                               Broker broker, StringLogger logger, ResponseReceiver responseReceiver, LastCommittedTxIdSetter lastCommittedTxIdSetter,
                               NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups)
    {
        super( storeDir, params, highlyAvailableGraphDatabase, broker, logger, nodeLookup, relationshipLookups );

        assert broker != null && logger != null && responseReceiver != null  && lastCommittedTxIdSetter != null &&
               nodeLookup != null && relationshipLookups != null;
        
        this.responseReceiver = responseReceiver;
        this.lastCommittedTxIdSetter = lastCommittedTxIdSetter;

        run();
    }

    @Override
    protected TxHook createTxHook()
    {
        return new SlaveTxHook( broker, responseReceiver, this );
    }

    @Override
    protected LastCommittedTxIdSetter createLastCommittedTxIdSetter()
    {
        return lastCommittedTxIdSetter;
    }

    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        assert txManager != null;
        return new SlaveTxIdGenerator( broker, responseReceiver, txManager );
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return slaveIdGeneratorFactory = new SlaveIdGenerator.SlaveIdGeneratorFactory( this.broker, this.responseReceiver );
    }

    @Override
    protected LockManager createLockManager()
    {
        assert txManager != null && txHook != null;
        return new SlaveLockManager( ragManager, (TxManager) txManager, txHook, broker, responseReceiver );
    }

    public void forgetIdAllocationsFromMaster()
    {
        slaveIdGeneratorFactory.forgetIdAllocationsFromMaster();
    }
}
