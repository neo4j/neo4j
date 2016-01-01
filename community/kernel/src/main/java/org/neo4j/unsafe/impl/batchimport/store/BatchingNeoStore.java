/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static java.lang.String.valueOf;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.configForStoreDir;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Mode.APPEND_ONLY;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Mode.UPDATE;

/**
 * Creator and accessor of {@link NeoStore} with some logic to provide very batch friendly services to the
 * {@link NeoStore} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStore implements AutoCloseable
{
    private final FileSystemAbstraction fileSystem;
    private final NeoStore neoStore;
    private final BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private final BatchingLabelTokenRepository labelRepository;
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final StringLogger logger;
    private final Config neo4jConfig;
    private final File neoStoreFileName;
    private final BatchingWindowPoolFactory pageCacheFactory;
    private final WriterFactory writerFactory;

    public BatchingNeoStore( FileSystemAbstraction fileSystem, String storeDir,
                                  Configuration config, Monitor writeMonitor, Logging logging,
                                  WriterFactory writerFactory )
    {
        this.fileSystem = fileSystem;
        this.writerFactory = writerFactory;
        this.neoStoreFileName = new File( storeDir, NeoStore.DEFAULT_NAME );

        this.logger = logging.getMessagesLog( getClass() );
        this.neo4jConfig = configForStoreDir(
                new Config( stringMap( dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ) ) ),
                new File( storeDir ) );

        this.pageCacheFactory = new BatchingWindowPoolFactory( config.fileChannelBufferSize(),
                writeMonitor, APPEND_ONLY, writerFactory );
        this.neoStore = newNeoStore( pageCacheFactory );
        flushNeoStoreAndAwaitEverythingWritten();
        this.propertyKeyRepository = new BatchingPropertyKeyTokenRepository( neoStore.getPropertyKeyTokenStore() );
        this.labelRepository = new BatchingLabelTokenRepository( neoStore.getLabelTokenStore() );
        this.relationshipTypeRepository =
                new BatchingRelationshipTypeTokenRepository( neoStore.getRelationshipTypeStore() );
    }

    private NeoStore newNeoStore( WindowPoolFactory windowPoolFactory )
    {
        StoreFactory storeFactory = new StoreFactory( neo4jConfig, new BatchingIdGeneratorFactory(),
                windowPoolFactory, fileSystem, logger, new DefaultTxHook() );
        if ( fileSystem.fileExists( neoStoreFileName ) )
        {
            return storeFactory.newNeoStore( neoStoreFileName );
        }
        return storeFactory.createNeoStore( neoStoreFileName );
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

    public void switchToUpdateMode()
    {
        pageCacheFactory.setMode( UPDATE );
    }

    @Override
    public void close()
    {
        // Flush out all pending changes
        propertyKeyRepository.close();
        labelRepository.close();
        relationshipTypeRepository.close();
        flushNeoStoreAndAwaitEverythingWritten();

        // Close the neo store
        neoStore.close();
    }

    private void flushNeoStoreAndAwaitEverythingWritten()
    {
        neoStore.flushAll();
        writerFactory.awaitEverythingWritten();
    }
}
