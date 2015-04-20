/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.store;

import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class DirectDiffRecordAccess extends DirectRecordAccess implements DiffRecordAccess
{
    public DirectDiffRecordAccess( DiffStore access )
    {
        super( access );
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return new DirectRecordReference<>( ((DiffStore) access).getMasterRecord(), this );
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return referenceTo( access.getNodeStore(), id );
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return referenceTo( access.getRelationshipStore(), id );
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return referenceTo( access.getPropertyStore(), id );
    }

    @Override
    public DynamicRecord changedSchema( long id )
    {
        return ((DiffStore) access).getSchemaStore().getChangedRecord( id );
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return ((DiffStore) access).getNodeStore().getChangedRecord( id );
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return ((DiffStore) access).getRelationshipStore().getChangedRecord( id );
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return ((DiffStore) access).getPropertyStore().getChangedRecord( id );
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return ((DiffStore) access).getStringStore().getChangedRecord( id );
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return ((DiffStore) access).getArrayStore().getChangedRecord( id );
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return new DirectRecordReference<>( access.getRawNeoStore().asRecord(), this );
    }

    @Override
    <RECORD extends AbstractBaseRecord> RecordReference<RECORD> referenceTo( RecordStore<RECORD> store, long id )
    {
        return new DirectDiffRecordReference<>( store.forceGetRecord( id ), store.forceGetRaw( id ), this  );
    }

    private static class DirectDiffRecordReference<RECORD extends AbstractBaseRecord> extends DirectRecordReference<RECORD>
    {
        private final RECORD oldRecord;

        DirectDiffRecordReference( RECORD newRecord, RECORD oldRecord, RecordAccess records )
        {
            super( newRecord, records );
            this.oldRecord = oldRecord;
        }

        @Override
        public void dispatch( PendingReferenceCheck<RECORD> reporter )
        {
            reporter.checkDiffReference( oldRecord, record, records );
        }
    }
}
