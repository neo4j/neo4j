/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.CommandCreationContext;

import static java.lang.Math.toIntExact;

/**
 * Holds commit data structures for creating records in a {@link NeoStores}.
 */
class RecordStorageCommandCreationContext implements CommandCreationContext
{
    private final NeoStores neoStores;
    private final Loaders loaders;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final SchemaStore schemaStore;

    RecordStorageCommandCreationContext( NeoStores neoStores, int denseNodeThreshold )
    {
        this.neoStores = neoStores;
        this.loaders = new Loaders( neoStores );
        RelationshipGroupGetter relationshipGroupGetter = new RelationshipGroupGetter( neoStores.getRelationshipGroupStore() );
        this.relationshipCreator = new RelationshipCreator( relationshipGroupGetter, denseNodeThreshold );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        this.propertyDeleter = new PropertyDeleter( propertyTraverser );
        this.relationshipDeleter = new RelationshipDeleter( relationshipGroupGetter, propertyDeleter );
        PropertyStore propertyStore = neoStores.getPropertyStore();
        this.propertyCreator = new PropertyCreator(
                new StandardDynamicRecordAllocator( propertyStore.getStringStore(), propertyStore.getStringStore().getRecordDataSize() ),
                new StandardDynamicRecordAllocator( propertyStore.getArrayStore(), propertyStore.getArrayStore().getRecordDataSize() ), propertyStore,
                propertyTraverser, propertyStore.allowStorePointsAndTemporal() );
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.schemaStore = neoStores.getSchemaStore();
    }

    private long nextId( StoreType storeType )
    {
        return neoStores.getRecordStore( storeType ).nextId();
    }

    @Override
    public long reserveNode()
    {
        return nextId( StoreType.NODE );
    }

    @Override
    public long reserveRelationship()
    {
        return nextId( StoreType.RELATIONSHIP );
    }

    @Override
    public long reserveSchema()
    {
        return nextId( StoreType.SCHEMA );
    }

    @Override
    public int reserveRelationshipTypeTokenId()
    {
        return toIntExact( neoStores.getRelationshipTypeTokenStore().nextId() );
    }

    @Override
    public int reservePropertyKeyTokenId()
    {
        return toIntExact( neoStores.getPropertyKeyTokenStore().nextId() );
    }

    @Override
    public int reserveLabelTokenId()
    {
        return toIntExact( neoStores.getLabelTokenStore().nextId() );
    }

    @Override
    public void close()
    {
    }

    TransactionRecordState createTransactionRecordState( IntegrityValidator integrityValidator, long lastTransactionIdWhenStarted,
            ResourceLocker locks )
    {
        RecordChangeSet recordChangeSet = new RecordChangeSet( loaders );
        return new TransactionRecordState( neoStores, integrityValidator,
                recordChangeSet, lastTransactionIdWhenStarted, locks,
                relationshipCreator, relationshipDeleter, propertyCreator, propertyDeleter );
    }
}
