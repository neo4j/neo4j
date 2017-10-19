/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.api.store.PropertyUtil;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

class AllStoreHolder extends Read implements Token
{
    private final RelationshipGroupStore groupStore;
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final StorageStatement statement;
    private final StoreReadLayer read;
    private final Lazy<ExplicitIndexTransactionState> explicitIndexes;

    AllStoreHolder( RecordStorageEngine engine, Supplier<ExplicitIndexTransactionState> explicitIndexes )
    {
        read = engine.storeReadLayer();
        statement = read.newStatement();
        NeoStores stores = engine.testAccessNeoStores();
        this.nodeStore = stores.getNodeStore();
        this.relationshipStore = stores.getRelationshipStore();
        this.groupStore = stores.getRelationshipGroupStore();
        this.propertyStore = stores.getPropertyStore();
        this.explicitIndexes = Suppliers.lazySingleton( explicitIndexes );
    }

    @Override
    long graphPropertiesReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    IndexReader indexReader( IndexDescriptor index )
    {
        try
        {
            return statement.getIndexReader( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    LabelScanReader labelScanReader()
    {
        return statement.getLabelScanReader();
    }

    @Override
    ExplicitIndex explicitNodeIndex( String indexName )
    {
        try
        {
            return explicitIndexes.get().nodeChanges( indexName );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // TODO: exception handling
            throw new RuntimeException( "SOMEONE HAS NOT IMPLEMENTED PROPER EXCEPTION HANDLING!", e );
        }
    }

    @Override
    ExplicitIndex explicitRelationshipIndex( String indexName )
    {
        try
        {
            return explicitIndexes.get().relationshipChanges( indexName );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // TODO: exception handling
            throw new RuntimeException( "SOMEONE HAS NOT IMPLEMENTED PROPER EXCEPTION HANDLING!", e );
        }
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        IndexDescriptor indexDescriptor = read.indexGetForSchema( new LabelSchemaDescriptor( label, properties ) );
        if ( indexDescriptor == null )
        {
            return CapableIndexReference.NO_INDEX;
        }
        try
        {
            IndexCapability indexCapability = read.indexGetCapability( indexDescriptor );
            return new IndexReference( indexDescriptor.type() == IndexDescriptor.Type.UNIQUE, indexCapability, label, properties );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void labelCreateForName( String labelName, int id ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void propertyKeyCreateForName( String propertyKeyName, int id ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipTypeCreateForName( String relationshipTypeName, int id ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int nodeLabel( String name )
    {
        return read.labelGetForName( name );
    }

    @Override
    public int relationshipType( String name )
    {
        return read.relationshipTypeGetForName( name );
    }

    @Override
    public int propertyKey( String name )
    {
        return read.propertyKeyGetForName( name );
    }

    @Override
    PageCursor nodePage( long reference )
    {
        return nodeStore.openPageCursorForReading( reference );
    }

    @Override
    PageCursor relationshipPage( long reference )
    {
        return relationshipStore.openPageCursorForReading( reference );
    }

    @Override
    PageCursor groupPage( long reference )
    {
        return groupStore.openPageCursorForReading( reference );
    }

    @Override
    PageCursor propertyPage( long reference )
    {
        return propertyStore.openPageCursorForReading( reference );
    }

    @Override
    PageCursor stringPage( long reference )
    {
        return propertyStore.getStringStore().openPageCursorForReading( reference );
    }

    @Override
    PageCursor arrayPage( long reference )
    {
        return propertyStore.getArrayStore().openPageCursorForReading( reference );
    }

    @Override
    RecordCursor<DynamicRecord> labelCursor()
    {
        return newCursor( nodeStore.getDynamicLabelStore() );
    }

    private static <R extends AbstractBaseRecord> RecordCursor<R> newCursor( RecordStore<R> store )
    {
        return store.newRecordCursor( store.newRecord() ).acquire( store.getNumberOfReservedLowIds(), NORMAL );
    }

    @Override
    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        nodeStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        propertyStore.getRecordByCursor( reference, record, RecordLoad.NORMAL, pageCursor );
    }

    @Override
    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        groupStore.getRecordByCursor( reference, record, RecordLoad.NORMAL, page );
    }

    @Override
    long nodeHighMark()
    {
        return nodeStore.getHighestPossibleIdInUse();
    }

    @Override
    long relationshipHighMark()
    {
        return relationshipStore.getHighestPossibleIdInUse();
    }

    @Override
    TextValue string( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer =
                cursor.buffer = readDynamic( propertyStore.getStringStore(), reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    @Override
    ArrayValue array( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer =
                cursor.buffer = readDynamic( propertyStore.getArrayStore(), reference, cursor.buffer, page );
        buffer.flip();
        return PropertyUtil.readArrayFromBuffer( buffer );
    }

    private static ByteBuffer readDynamic(
            AbstractDynamicStore store, long reference, ByteBuffer buffer,
            PageCursor page )
    {
        if ( buffer == null )
        {
            buffer = ByteBuffer.allocate( 512 );
        }
        else
        {
            buffer.clear();
        }
        DynamicRecord record = store.newRecord();
        do
        {
            store.getRecordByCursor( reference, record, RecordLoad.CHECK, page );
            reference = record.getNextBlock();
            byte[] data = record.getData();
            if ( buffer.remaining() < data.length )
            {
                buffer = grow( buffer, data.length );
            }
            buffer.put( data, 0, data.length );
        }
        while ( reference != NO_ID );
        return buffer;
    }

    private static ByteBuffer grow( ByteBuffer buffer, int required )
    {
        buffer.flip();
        int capacity = buffer.capacity();
        do
        {
            capacity *= 2;
        }
        while ( capacity - buffer.limit() < required );
        return ByteBuffer.allocate( capacity ).order( ByteOrder.LITTLE_ENDIAN ).put( buffer );
    }
}
