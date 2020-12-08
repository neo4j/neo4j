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

import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;
import org.neo4j.internal.recordstorage.RecordAccess;
import org.neo4j.internal.recordstorage.RecordAccessSet;
import org.neo4j.internal.schema.SchemaRule;
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
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.RelationshipDirection;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class Tracker extends NoOpClient implements RecordAccessSet, RelationshipGroupDegreesStore.Updater
{
    private final RecordAccessSet delegate;
    private final TrackingRecordAccess<RelationshipRecord,Void> relRecords;
    private final Set<Long> relationshipLocksAcquired = new HashSet<>();

    Tracker( NeoStores neoStores, IdGeneratorFactory idGeneratorFactory )
    {
        this.delegate = new DirectRecordAccessSet( neoStores, idGeneratorFactory, NULL );
        this.relRecords = new TrackingRecordAccess<>( delegate.getRelRecords(), this );
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        assertEquals( ResourceTypes.RELATIONSHIP, resourceType );
        for ( long resourceId : resourceIds )
        {
            relationshipLocksAcquired.add( resourceId );
        }
    }

    void changingRelationship( long relId )
    {   // Called by tracking record proxies
        assertTrue( relationshipLocksAcquired.contains( relId ), "Tried to change relationship " + relId + " without this transaction having it locked" );
    }

    @Override
    public RecordAccess<NodeRecord,Void> getNodeRecords()
    {
        return delegate.getNodeRecords();
    }

    @Override
    public RecordAccess<PropertyRecord,PrimitiveRecord> getPropertyRecords()
    {
        return delegate.getPropertyRecords();
    }

    @Override
    public RecordAccess<RelationshipRecord,Void> getRelRecords()
    {
        return relRecords;
    }

    @Override
    public RecordAccess<RelationshipGroupRecord,Integer> getRelGroupRecords()
    {
        return delegate.getRelGroupRecords();
    }

    @Override
    public RecordAccess<SchemaRecord,SchemaRule> getSchemaRuleChanges()
    {
        return delegate.getSchemaRuleChanges();
    }

    @Override
    public RecordAccess<PropertyKeyTokenRecord,Void> getPropertyKeyTokenChanges()
    {
        return delegate.getPropertyKeyTokenChanges();
    }

    @Override
    public RecordAccess<LabelTokenRecord,Void> getLabelTokenChanges()
    {
        return delegate.getLabelTokenChanges();
    }

    @Override
    public RecordAccess<RelationshipTypeTokenRecord,Void> getRelationshipTypeTokenChanges()
    {
        return delegate.getRelationshipTypeTokenChanges();
    }

    @Override
    public void increment( long groupId, RelationshipDirection direction, long delta )
    {
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
