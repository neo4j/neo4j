/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RelationshipCreatorTest
{
    @Test
    public void shouldOnlyChangeLockedRecordsWhenUpgradingToDenseNode() throws Exception
    {
        // GIVEN
        long nodeId = createNodeWithRelationships( DENSE_NODE_THRESHOLD );
        NeoStores neoStores = flipToNeoStores();

        Tracker tracker = new Tracker( neoStores );
        RelationshipGroupGetter groupGetter = new RelationshipGroupGetter( neoStores.getRelationshipGroupStore() );
        RelationshipCreator relationshipCreator = new RelationshipCreator( tracker, groupGetter, 5 );

        // WHEN
        relationshipCreator.relationshipCreate( idGeneratorFactory.get( IdType.RELATIONSHIP ).nextId(), 0,
                nodeId, nodeId, tracker );

        // THEN
        assertEquals( tracker.relationshipLocksAcquired.size(), tracker.changedRelationships.size() );
        assertFalse( tracker.relationshipLocksAcquired.isEmpty() );
    }

    private NeoStores flipToNeoStores()
    {
        return dbRule.getGraphDatabaseAPI().getDependencyResolver().resolveDependency(
                NeoStoresSupplier.class ).get();
    }

    private long createNodeWithRelationships( int count )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            for ( int i = 0; i < count; i++ )
            {
                node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            }
            tx.success();
            return node.getId();
        }
    }

    static class Tracker extends NoOpClient implements RecordAccessSet
    {
        private final RecordAccessSet delegate;
        private final TrackingRecordAccess<RelationshipRecord, Void> relRecords;
        private final Set<Long> relationshipLocksAcquired = new HashSet<>();
        private final Set<Long> changedRelationships = new HashSet<>();

        public Tracker( NeoStores neoStores )
        {
            this.delegate = new DirectRecordAccessSet( neoStores );
            this.relRecords = new TrackingRecordAccess<>( delegate.getRelRecords(), this );
        }

        @Override
        public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
                throws AcquireLockTimeoutException
        {
            assertEquals( ResourceTypes.RELATIONSHIP, resourceType );
            for ( long resourceId : resourceIds )
            {
                relationshipLocksAcquired.add( resourceId );
            }
        }

        protected void changingRelationship( long relId )
        {   // Called by tracking record proxies
            assertTrue( "Tried to change relationship " + relId + " without this transaction having it locked",
                    relationshipLocksAcquired.contains( relId ) );
            changedRelationships.add( relId );
        }

        @Override
        public RecordAccess<Long, NodeRecord, Void> getNodeRecords()
        {
            return delegate.getNodeRecords();
        }

        @Override
        public RecordAccess<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
        {
            return delegate.getPropertyRecords();
        }

        @Override
        public RecordAccess<Long, RelationshipRecord, Void> getRelRecords()
        {
            return relRecords;
        }

        @Override
        public RecordAccess<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
        {
            return delegate.getRelGroupRecords();
        }

        @Override
        public RecordAccess<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
        {
            return delegate.getSchemaRuleChanges();
        }

        @Override
        public RecordAccess<Integer, PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
        {
            return delegate.getPropertyKeyTokenChanges();
        }

        @Override
        public RecordAccess<Integer, LabelTokenRecord, Void> getLabelTokenChanges()
        {
            return delegate.getLabelTokenChanges();
        }

        @Override
        public RecordAccess<Integer, RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
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
    }

    private static final int DENSE_NODE_THRESHOLD = 5;
    public final @Rule DatabaseRule dbRule = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.dense_node_threshold, String.valueOf( DENSE_NODE_THRESHOLD ) );
        }
    };
    private IdGeneratorFactory idGeneratorFactory;

    @Before
    public void before()
    {
        idGeneratorFactory = dbRule.getGraphDatabaseAPI().getDependencyResolver().resolveDependency(
                IdGeneratorFactory.class );
    }
}
