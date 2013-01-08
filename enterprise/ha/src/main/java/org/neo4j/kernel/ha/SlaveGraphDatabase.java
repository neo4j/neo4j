/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.logging.Logging;

/**
 * Slave implementation of HA Graph Database
 */
public class SlaveGraphDatabase
    extends AbstractHAGraphDatabase
{
    private final SlaveDatabaseOperations databaseOperations;
    private LastCommittedTxIdSetter lastCommittedTxIdSetter;
    private SlaveIdGenerator.SlaveIdGeneratorFactory slaveIdGeneratorFactory;
    private FileSystemAbstraction fileSystemAbstraction;

    public SlaveGraphDatabase( String storeDir, Map<String, String> params,
            StoreId storeId, HighlyAvailableGraphDatabase highlyAvailableGraphDatabase, Broker broker,
            Logging logging, SlaveDatabaseOperations databaseOperations,
            LastCommittedTxIdSetter lastCommittedTxIdSetter, NodeProxy.NodeLookup nodeLookup,
            RelationshipProxy.RelationshipLookups relationshipLookups,
            FileSystemAbstraction fileSystemAbstraction,
            Iterable<IndexProvider> indexProviders, Iterable<KernelExtension> kernelExtensions,
            Iterable<CacheProvider> cacheProviders, Caches caches )
    {
        super( storeDir, params, storeId, highlyAvailableGraphDatabase, broker, logging, nodeLookup, relationshipLookups,
                indexProviders, kernelExtensions, cacheProviders, caches );
        this.fileSystemAbstraction = fileSystemAbstraction;

        assert broker != null && logging != null && databaseOperations != null  && lastCommittedTxIdSetter != null &&
               nodeLookup != null && relationshipLookups != null;

        this.databaseOperations = databaseOperations;
        this.lastCommittedTxIdSetter = lastCommittedTxIdSetter;

        run();
    }

    @Override
    protected TxHook createTxHook()
    {
        return new SlaveTxHook( broker, databaseOperations, this );
    }

    @Override
    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return fileSystemAbstraction;
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
        return new SlaveTxIdGenerator( broker, databaseOperations, txManager );
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return slaveIdGeneratorFactory = new SlaveIdGenerator.SlaveIdGeneratorFactory( broker, databaseOperations );
    }

    @Override
    protected LockManager createLockManager()
    {
        assert txManager != null && txHook != null;
        return new SlaveLockManager( ragManager, (TxManager) txManager, txHook, broker, databaseOperations );
    }

    public void forgetIdAllocationsFromMaster()
    {
        slaveIdGeneratorFactory.forgetIdAllocationsFromMaster();
    }
    
    @Override
    protected RelationshipTypeCreator createRelationshipTypeCreator()
    {
        return new SlaveRelationshipTypeCreator( broker, databaseOperations );
    }
}
