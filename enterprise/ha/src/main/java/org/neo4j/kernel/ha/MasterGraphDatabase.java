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

import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.logging.Logging;

/**
 * TODO
 */
public class MasterGraphDatabase
    extends AbstractHAGraphDatabase
{
    private StoreId storeId;

    public MasterGraphDatabase( String storeDir, Map<String, String> params,
                                StoreId storeId, HighlyAvailableGraphDatabase highlyAvailableGraphDatabase,
                                Broker broker, Logging logging,
                                NodeProxy.NodeLookup nodeLookup,
                                RelationshipProxy.RelationshipLookups relationshipLookups,
                                Iterable<IndexProvider> indexProviders, Iterable<KernelExtension> kernelExtensions,
                                Iterable<CacheProvider> cacheProviders, Caches caches )
    {
        super( storeDir, params, storeId, highlyAvailableGraphDatabase, broker, logging, nodeLookup, relationshipLookups,
                indexProviders, kernelExtensions, cacheProviders, caches );
        this.storeId = storeId;

        run();
    }

    @Override
    protected StoreFactory createStoreFactory()
    {
        return new StoreFactory(config, idGeneratorFactory, fileSystem, lastCommittedTxIdSetter, msgLog, txHook)
        {
            @Override
            public NeoStore createNeoStore( String fileName )
            {
                return super.createNeoStore( fileName, storeId );
            }
        };
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new MasterIdGeneratorFactory();
    }

    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        return new MasterTxIdGenerator( broker );
    }

    @Override
    protected TxHook createTxHook()
    {
        return new MasterTxHook(super.createTxHook());
    }

    @Override
    protected LastCommittedTxIdSetter createLastCommittedTxIdSetter()
    {
        return new ZooKeeperLastCommittedTxIdSetter( broker );
    }
}
