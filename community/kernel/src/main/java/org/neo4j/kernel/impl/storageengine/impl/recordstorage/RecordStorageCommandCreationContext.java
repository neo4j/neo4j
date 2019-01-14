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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.RenewableBatchIdSequences;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyDeleter;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreator;
import org.neo4j.kernel.impl.transaction.state.RelationshipDeleter;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.lock.ResourceLocker;

/**
 * Holds commit data structures for creating records in a {@link NeoStores}.
 */
public class RecordStorageCommandCreationContext implements CommandCreationContext
{
    private final NeoStores neoStores;
    private final Loaders loaders;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final RenewableBatchIdSequences idBatches;

    RecordStorageCommandCreationContext( NeoStores neoStores, int denseNodeThreshold, int idBatchSize )
    {
        this.neoStores = neoStores;
        this.idBatches = new RenewableBatchIdSequences( neoStores, idBatchSize );

        this.loaders = new Loaders( neoStores );
        RelationshipGroupGetter relationshipGroupGetter =
                new RelationshipGroupGetter( idBatches.idGenerator( StoreType.RELATIONSHIP_GROUP ) );
        this.relationshipCreator = new RelationshipCreator( relationshipGroupGetter, denseNodeThreshold );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        this.propertyDeleter = new PropertyDeleter( propertyTraverser );
        this.relationshipDeleter = new RelationshipDeleter( relationshipGroupGetter, propertyDeleter );
        this.propertyCreator = new PropertyCreator(
                new StandardDynamicRecordAllocator( idBatches.idGenerator( StoreType.PROPERTY_STRING ),
                        neoStores.getPropertyStore().getStringStore().getRecordDataSize() ),
                new StandardDynamicRecordAllocator( idBatches.idGenerator( StoreType.PROPERTY_ARRAY ),
                        neoStores.getPropertyStore().getArrayStore().getRecordDataSize() ),
                idBatches.idGenerator( StoreType.PROPERTY ),
                propertyTraverser, neoStores.getPropertyStore().allowStorePointsAndTemporal() );
    }

    public long nextId( StoreType storeType )
    {
        return idBatches.nextId( storeType );
    }

    @Override
    public void close()
    {
        this.idBatches.close();
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
