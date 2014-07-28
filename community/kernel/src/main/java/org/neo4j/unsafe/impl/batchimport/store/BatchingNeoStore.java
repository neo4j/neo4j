/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Mode;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static java.lang.String.valueOf;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.configForStoreDir;

/**
 * Creator and accessor of {@link NeoStore} with some logic to provide very batch friendly services to the
 * {@link NeoStore} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStore implements AutoCloseable
{
    private final LifeSupport life = new LifeSupport();
    private final ChannelReusingFileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private NeoStore neoStore;
    private final BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private final BatchingLabelTokenRepository labelRepository;
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final StringLogger logger;
    private final Config neo4jConfig;
    private final Configuration config;
    private final Monitor writeMonitor;
    private final WriterFactory writerFactory;

    public BatchingNeoStore( FileSystemAbstraction fileSystem, String storeDir,
                             Configuration config, Monitor writeMonitor, Logging logging,
                             WriterFactory writerFactory, Monitors monitors )
    {
        this.fileSystem = life.add( new ChannelReusingFileSystemAbstraction( fileSystem ) );
        this.monitors = monitors;
        this.config = config;
        this.writeMonitor = writeMonitor;
        this.writerFactory = writerFactory;
        this.logger = logging.getMessagesLog( getClass() );
        this.neo4jConfig = configForStoreDir(
                new Config( stringMap( dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ) ),
                        GraphDatabaseSettings.class ),
                new File( storeDir ) );

        this.neoStore = newBatchWritingNeoStore();
        this.propertyKeyRepository = new BatchingPropertyKeyTokenRepository( neoStore.getPropertyKeyTokenStore() );
        this.labelRepository = new BatchingLabelTokenRepository( neoStore.getLabelTokenStore() );
        this.relationshipTypeRepository =
                new BatchingRelationshipTypeTokenRepository( neoStore.getRelationshipTypeTokenStore() );
        life.start();
    }

    private NeoStore newNeoStore( PageCache pageCache )
    {
        StoreFactory storeFactory = new StoreFactory( neo4jConfig, new BatchingIdGeneratorFactory(),
                pageCache, fileSystem, logger, monitors );
        return storeFactory.newNeoStore( true );
    }

    private NeoStore newBatchWritingNeoStore()
    {
        return newNeoStore( batchingPageCache( Mode.APPEND_ONLY ) );
    }

    private BatchingPageCache batchingPageCache( Mode mode )
    {
        return new BatchingPageCache( fileSystem, config.fileChannelBufferSize(), writerFactory, writeMonitor, mode );
    }

    private NeoStore newReverseUpdatingNeoStore()
    {
        TailoredPageCache factory = new TailoredPageCache( batchingPageCache( Mode.APPEND_ONLY ) );
        PageCache batchUpdatingFactory = batchingPageCache( Mode.UPDATE );
        factory.override( StoreFactory.NODE_STORE_NAME, batchUpdatingFactory );
        factory.override( StoreFactory.RELATIONSHIP_STORE_NAME, batchUpdatingFactory );

        return newNeoStore( factory );
    }

    public NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    public PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    public BatchingTokenRepository<?> getPropertyKeyRepository()
    {
        return propertyKeyRepository;
    }

    public BatchingTokenRepository<?> getLabelRepository()
    {
        return labelRepository;
    }

    public BatchingTokenRepository<?> getRelationshipTypeRepository()
    {
        return relationshipTypeRepository;
    }

    public RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return neoStore.getRelationshipGroupStore();
    }

    public void switchNodeAndRelationshipStoresToUpdateMode()
    {
        // Close token repositories, not needed beyond this point.
        propertyKeyRepository.close();
        labelRepository.close();
        relationshipTypeRepository.close();
        // Close neo store as a whole
        neoStore.close();
        neoStore = null;

        // Open store optimized for reverse update batching
        neoStore = newReverseUpdatingNeoStore();
    }

    public void flushAll()
    {
        if ( neoStore != null )
        {
            neoStore.flush();
        }
    }

    @Override
    public void close()
    {
        if ( neoStore != null )
        {
            neoStore.close();
            neoStore = null;
        }
        life.shutdown();
    }
}
