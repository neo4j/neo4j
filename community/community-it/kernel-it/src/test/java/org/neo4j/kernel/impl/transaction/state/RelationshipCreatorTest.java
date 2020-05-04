/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;
import org.neo4j.internal.recordstorage.RecordAccess;
import org.neo4j.internal.recordstorage.RecordAccessSet;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RelationshipCreator;
import org.neo4j.internal.recordstorage.RelationshipGroupGetter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class RelationshipCreatorTest
{
    private static final int DENSE_NODE_THRESHOLD = 5;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private IdGeneratorFactory idGeneratorFactory;
    @Inject
    private RecordStorageEngine storageEngine;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.dense_node_threshold, DENSE_NODE_THRESHOLD );
    }

    @Test
    void shouldOnlyChangeLockedRecordsWhenUpgradingToDenseNode()
    {
        // GIVEN
        long nodeId = createNodeWithRelationships( DENSE_NODE_THRESHOLD );
        NeoStores neoStores = flipToNeoStores();

        Tracker tracker = new Tracker( neoStores, idGeneratorFactory );
        RelationshipGroupGetter groupGetter = new RelationshipGroupGetter( neoStores.getRelationshipGroupStore(), NULL );
        RelationshipCreator relationshipCreator = new RelationshipCreator( groupGetter, 5, NULL );

        // WHEN
        relationshipCreator.relationshipCreate( idGeneratorFactory.get( IdType.RELATIONSHIP ).nextId( NULL ), 0,
                nodeId, nodeId, tracker, tracker );

        // THEN
        assertEquals( tracker.relationshipLocksAcquired.size(), tracker.changedRelationships.size() );
        assertFalse( tracker.relationshipLocksAcquired.isEmpty() );
    }

    private NeoStores flipToNeoStores()
    {
        return storageEngine.testAccessNeoStores();
    }

    private long createNodeWithRelationships( int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            for ( int i = 0; i < count; i++ )
            {
                node.createRelationshipTo( tx.createNode(), MyRelTypes.TEST );
            }
            tx.commit();
            return node.getId();
        }
    }

    static class Tracker extends NoOpClient implements RecordAccessSet
    {
        private final RecordAccessSet delegate;
        private final TrackingRecordAccess<RelationshipRecord, Void> relRecords;
        private final Set<Long> relationshipLocksAcquired = new HashSet<>();
        private final Set<Long> changedRelationships = new HashSet<>();

        Tracker( NeoStores neoStores, IdGeneratorFactory idGeneratorFactory )
        {
            this.delegate = new DirectRecordAccessSet( neoStores, idGeneratorFactory );
            this.relRecords = new TrackingRecordAccess<>( delegate.getRelRecords(), this );
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
                throws AcquireLockTimeoutException
        {
            assertEquals( ResourceTypes.RELATIONSHIP, resourceType );
            for ( long resourceId : resourceIds )
            {
                relationshipLocksAcquired.add( resourceId );
            }
        }

        void changingRelationship( long relId )
        {   // Called by tracking record proxies
            assertTrue( relationshipLocksAcquired.contains( relId ),
                    "Tried to change relationship " + relId + " without this transaction having it locked" );
            changedRelationships.add( relId );
        }

        @Override
        public RecordAccess<NodeRecord, Void> getNodeRecords()
        {
            return delegate.getNodeRecords();
        }

        @Override
        public RecordAccess<PropertyRecord, PrimitiveRecord> getPropertyRecords()
        {
            return delegate.getPropertyRecords();
        }

        @Override
        public RecordAccess<RelationshipRecord, Void> getRelRecords()
        {
            return relRecords;
        }

        @Override
        public RecordAccess<RelationshipGroupRecord, Integer> getRelGroupRecords()
        {
            return delegate.getRelGroupRecords();
        }

        @Override
        public RecordAccess<SchemaRecord, SchemaRule> getSchemaRuleChanges()
        {
            return delegate.getSchemaRuleChanges();
        }

        @Override
        public RecordAccess<PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
        {
            return delegate.getPropertyKeyTokenChanges();
        }

        @Override
        public RecordAccess<LabelTokenRecord, Void> getLabelTokenChanges()
        {
            return delegate.getLabelTokenChanges();
        }

        @Override
        public RecordAccess<RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
        {
            return delegate.getRelationshipTypeTokenChanges();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean hasChanges()
        {
            return delegate.hasChanges();
        }

        @Override
        public int changeSize()
        {
            return delegate.changeSize();
        }
    }
}
