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
package org.neo4j.kernel.impl.storemigration;

import static java.lang.Integer.parseInt;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

import java.io.File;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Convenience around a {@link StoreFactory} where individual stores can be created lazily and all closed
 * Useful in store migration where only some stores are migrated, usually.
 * in {@link #close()}.
 */
public class IndividualNeoStores
{
    private final File storageFileName;
    private final Config config;
    private final StoreFactory storeFactory;

    private NodeStore nodeStore;
    private RelationshipStore relationshipStore;
    private RelationshipGroupStore relationshipGroupStore;

    public IndividualNeoStores( FileSystemAbstraction fileSystem, File storageFileName,
            Config config, IdGeneratorFactory idGeneratorFactory )
    {
        this.storageFileName = storageFileName;
        this.config = StoreFactory.configForNeoStore( config, storageFileName );
        this.storeFactory = new StoreFactory( this.config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL );
    }

    public File getNeoStoreFileName()
    {
        return storageFileName;
    }

    public File getNeoStoreDirectory()
    {
        return storageFileName.getParentFile();
    }

    public NodeStore createNodeStore()
    {
        storeFactory.createNodeStore();
        return nodeStore = storeFactory.newNodeStore();
    }

    public RelationshipStore createRelationshipStore()
    {
        storeFactory.createRelationshipStore();
        return relationshipStore = storeFactory.newRelationshipStore();
    }

    public RelationshipGroupStore createRelationshipGroupStore()
    {
        storeFactory.createRelationshipGroupStore( parseInt( dense_node_threshold.getDefaultValue() ) );
        return relationshipGroupStore = storeFactory.newRelationshipGroupStore();
    }

    // ... add more stores as you see fit

    public void close()
    {
        nullSafeClose( nodeStore, relationshipStore, relationshipGroupStore );
    }

    private void nullSafeClose( CommonAbstractStore... stores )
    {
        for ( CommonAbstractStore store : stores )
        {
            if ( store != null )
            {
                store.close();
            }
        }
    }
}
