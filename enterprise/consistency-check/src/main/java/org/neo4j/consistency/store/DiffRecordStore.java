/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency.store;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;

/**
 * Not thread safe, intended for single threaded use.
 */
public class DiffRecordStore<R extends AbstractBaseRecord> implements RecordStore<R>, Iterable<Long>
{
    private final RecordStore<R> actual;
    private final Map<Long, R> diff;
    private long highId = -1;

    public DiffRecordStore( RecordStore<R> actual )
    {
        this.actual = actual;
        this.diff = new HashMap<Long, R>();
    }

    @Override
    public String toString()
    {
        return "Diff/" + actual;
    }

    public void markDirty( long id )
    {
        if ( !diff.containsKey( id ) ) diff.put( id, null );
    }

    public boolean isModified( long id )
    {
        return diff.get( id ) != null;
    }

    public R forceGetRaw( R record )
    {
        if ( diff.containsKey( record.getLongId() ) )
        {
            return actual.forceGetRecord( record.getLongId() );
        }
        else
        {
            return record;
        }
    }

    @Override
    public R forceGetRaw( long id )
    {
        return actual.forceGetRecord( id );
    }

    @Override
    public int getRecordHeaderSize()
    {
        return actual.getRecordHeaderSize();
    }

    @Override
    public int getRecordSize()
    {
        return actual.getRecordSize();
    }

    @Override
    public File getStorageFileName()
    {
        return actual.getStorageFileName();
    }

    @Override
    public WindowPoolStats getWindowPoolStats()
    {
        return actual.getWindowPoolStats();
    }

    @Override
    public long getHighId()
    {
        return Math.max( highId, actual.getHighId() );
    }

    public long getRawHighId()
    {
        return actual.getHighId();
    }

    @Override
    public R getRecord( long id )
    {
        return getRecord( id, false );
    }

    @Override
    public R forceGetRecord( long id )
    {
        return getRecord( id, true );
    }

    private R getRecord( long id, boolean force )
    {
        R record = diff.get( id );
        if ( record == null ) return force ? actual.forceGetRecord( id ) : actual.getRecord( id );
        if ( !force && !record.inUse() ) throw new InvalidRecordException( record.getClass().getSimpleName() + "[" + id + "] not in use" );
        return record;
    }

    @Override
    public void updateRecord( R record )
    {
        if ( record.getLongId() > highId ) highId = record.getLongId();
        diff.put( record.getLongId(), record );
    }

    @Override
    public void forceUpdateRecord( R record )
    {
        updateRecord( record );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, R record ) throws FAILURE
    {
        actual.accept( new DispatchProcessor<FAILURE>( this, processor ), record );
    }

    @Override
    public Iterator<Long> iterator()
    {
        return diff.keySet().iterator();
    }

    @Override
    public void close()
    {
        diff.clear();
        actual.close();
    }

    public R getChangedRecord( long id )
    {
        return diff.get( id );
    }

    @SuppressWarnings( "unchecked" )
    private static class DispatchProcessor<FAILURE extends Exception> extends RecordStore.Processor<FAILURE>
    {
        private final DiffRecordStore<?> diffStore;
        private final RecordStore.Processor<FAILURE> processor;

        DispatchProcessor( DiffRecordStore<?> diffStore, RecordStore.Processor<FAILURE> processor )
        {
            this.diffStore = diffStore;
            this.processor = processor;
        }

        @Override
        public void processNode( RecordStore<NodeRecord> store, NodeRecord node ) throws FAILURE
        {
            processor.processNode( (RecordStore<NodeRecord>) diffStore, node );
        }

        @Override
        public void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel ) throws FAILURE
        {
            processor.processRelationship( (RecordStore<RelationshipRecord>) diffStore, rel );
        }

        @Override
        public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property ) throws FAILURE
        {
            processor.processProperty( (RecordStore<PropertyRecord>) diffStore, property );
        }

        @Override
        public void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType ) throws FAILURE
        {
            processor.processString( (RecordStore<DynamicRecord>) diffStore, string, idType );
        }

        @Override
        public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array ) throws FAILURE
        {
            processor.processArray( (RecordStore<DynamicRecord>) diffStore, array );
        }

        @Override
        public void processRelationshipType( RecordStore<RelationshipTypeTokenRecord> store, RelationshipTypeTokenRecord record ) throws FAILURE
        {
            processor.processRelationshipType( (RecordStore<RelationshipTypeTokenRecord>) diffStore, record );
        }

        @Override
        public void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record ) throws FAILURE
        {
            processor.processPropertyKeyToken( (RecordStore<PropertyKeyTokenRecord>) diffStore, record );
        }
    }
}
