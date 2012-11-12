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
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.logging.Logging;

/**
 * TODO
 */
public class AbstractHAGraphDatabase
    extends InternalAbstractGraphDatabase
{
    protected Broker broker;
    private NodeProxy.NodeLookup nodeLookup;
    private RelationshipProxy.RelationshipLookups relationshipLookups;
    private HighlyAvailableGraphDatabase highlyAvailableGraphDatabase;
    private final Caches caches;

    public AbstractHAGraphDatabase( String storeDir, Map<String, String> params,
                                    StoreId storeId, HighlyAvailableGraphDatabase highlyAvailableGraphDatabase,
                                    Broker broker, Logging logging,
                                    NodeProxy.NodeLookup nodeLookup,
                                    RelationshipProxy.RelationshipLookups relationshipLookups,
                                    Iterable<IndexProvider> indexProviders, Iterable<KernelExtension> kernelExtensions,
                                    Iterable<CacheProvider> cacheProviders, Caches caches )
    {
        super( storeDir, params, indexProviders, kernelExtensions, cacheProviders );
        this.highlyAvailableGraphDatabase = highlyAvailableGraphDatabase;
        this.caches = caches;
        this.storeId = storeId;

        assert broker != null && logging != null && nodeLookup != null && relationshipLookups != null;

        this.broker = broker;
        this.logging = logging;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
    }

    @Override
    protected KernelData createKernelData()
    {
        return new DefaultKernelData( config, this );
    }

    @Override
    protected NodeProxy.NodeLookup createNodeLookup()
    {
        return nodeLookup;
    }

    @Override
    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return relationshipLookups;
    }

    @Override
    protected Logging createStringLogger()
    {
        return logging;
    }

    public HighlyAvailableGraphDatabase getHighlyAvailableGraphDatabase()
    {
        return highlyAvailableGraphDatabase;
    }
    
    @Override
    public StoreId getStoreId()
    {
        return storeId;
    }
    
    @Override
    protected Caches createCaches()
    {
        return caches;
    }
}
