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
import java.util.Map;

import org.neo4j.function.Suppliers;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.store.PropertyUtil;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

public class AllStoreHolder extends Read implements Token
{
    private final StorageStatement.Nodes nodes;
    private final StorageStatement.Groups groups;
    private final StorageStatement.Properties properties;
    private final StorageStatement.Relationships relationships;
    private final StorageStatement statement;
    private final StoreReadLayer storeReadLayer;
    private final ExplicitIndexStore explicitIndexStore;
    private final Lazy<ExplicitIndexTransactionState> explicitIndexes;

    public AllStoreHolder( StorageEngine engine,
            StorageStatement statement,
            TxStateHolder txStateHolder,
            Cursors cursors,
            ExplicitIndexStore explicitIndexStore,
            AssertOpen assertOpen )
    {
        super( cursors, txStateHolder, assertOpen );
        this.storeReadLayer = engine.storeReadLayer();
        this.statement = statement; // use provided statement, to assert no leakage
        this.explicitIndexes = Suppliers.lazySingleton( txStateHolder::explicitIndexTxState );

        this.nodes = statement.nodes();
        this.relationships = statement.relationships();
        this.groups = statement.groups();
        this.properties = statement.properties();
        this.explicitIndexStore = explicitIndexStore;
    }

    @Override
    public boolean nodeExists( long id )
    {
        assertOpen.assertOpen();

        if ( hasTxStateWithChanges() )
        {
            TransactionState txState = txState();
            if ( txState.nodeIsDeletedInThisTx( id ) )
            {
                return false;
            }
            else if ( txState.nodeIsAddedInThisTx( id ) )
            {
                return true;
            }
        }
        return storeReadLayer.nodeExists( id );
    }

    @Override
    long graphPropertiesReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    IndexReader indexReader( org.neo4j.internal.kernel.api.IndexReference index )
    {
        try
        {
            IndexDescriptor indexDescriptor = index.isUnique() ?
                                              IndexDescriptorFactory.uniqueForLabel( index.label(), index.properties() ) :
                                              IndexDescriptorFactory.forLabel( index.label(), index.properties() );
            return statement.getIndexReader( indexDescriptor );
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
    ExplicitIndex explicitNodeIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexes.get().nodeChanges( indexName );
    }

    @Override
    ExplicitIndex explicitRelationshipIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexes.get().relationshipChanges( indexName );
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        assertOpen.assertOpen();
        IndexDescriptor indexDescriptor = storeReadLayer.indexGetForSchema( new LabelSchemaDescriptor( label, properties ) );
        if ( indexDescriptor == null )
        {
            return CapableIndexReference.NO_INDEX;
        }
        boolean unique = indexDescriptor.type() == IndexDescriptor.Type.UNIQUE;
        try
        {
            IndexCapability indexCapability = storeReadLayer.indexGetCapability( indexDescriptor );
            return new DefaultCapableIndexReference( unique, indexCapability, label, properties );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Could not find capability for index " + indexDescriptor, e );
        }
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws KernelException
    {
        return storeReadLayer.labelGetOrCreateForName( checkValidTokenName( labelName ) );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws KernelException
    {
        return storeReadLayer.propertyKeyGetOrCreateForName( propertyKeyName );
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
    public String labelGetName( int token ) throws LabelNotFoundKernelException
    {
        return storeReadLayer.labelGetName( token );
    }

    @Override
    public int labelGetForName( String name ) throws LabelNotFoundKernelException
    {
        return storeReadLayer.labelGetForName( name );
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
        return storeReadLayer.labelGetForName( name );
    }

    @Override
    public int relationshipType( String name )
    {
        return storeReadLayer.relationshipTypeGetForName( name );
    }

    @Override
    public int propertyKey( String name )
    {
        return storeReadLayer.propertyKeyGetForName( name );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        return storeReadLayer.propertyKeyGetName( propertyKeyId );
    }

    @Override
    PageCursor nodePage( long reference )
    {
        return nodes.openPageCursorForReading( reference );
    }

    @Override
    PageCursor relationshipPage( long reference )
    {
        return relationships.openPageCursorForReading( reference );
    }

    @Override
    PageCursor groupPage( long reference )
    {
        return groups.openPageCursorForReading( reference );
    }

    @Override
    PageCursor propertyPage( long reference )
    {
        return properties.openPageCursorForReading( reference );
    }

    @Override
    PageCursor stringPage( long reference )
    {
        return properties.openStringPageCursor( reference );
    }

    @Override
    PageCursor arrayPage( long reference )
    {
        return properties.openArrayPageCursor( reference );
    }

    @Override
    RecordCursor<DynamicRecord> labelCursor()
    {
        return nodes.newLabelCursor();
    }

    @Override
    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        nodes.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        relationships.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        //We need to load forcefully here since otherwise we can have inconsistent reads
        //for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
        properties.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    @Override
    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        groups.getRecordByCursor( reference, record, RecordLoad.NORMAL, page );
    }

    @Override
    long nodeHighMark()
    {
        return nodes.getHighestPossibleIdInUse();
    }

    @Override
    long relationshipHighMark()
    {
        return relationships.getHighestPossibleIdInUse();
    }

    @Override
    TextValue string( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = properties.loadString( reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    @Override
    ArrayValue array( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = properties.loadArray( reference, cursor.buffer, page );
        buffer.flip();
        return PropertyUtil.readArrayFromBuffer( buffer );
    }

    boolean nodeExistsInStore( long id )
    {
        return storeReadLayer.nodeExists( id );
    }

    void getOrCreateNodeIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    void getOrCreateRelationshipIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    private String checkValidTokenName( String name ) throws IllegalTokenNameException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new IllegalTokenNameException( name );
        }
        return name;
    }
}
