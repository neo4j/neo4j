/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.register.Registers.newDoubleLongRegister;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 */
public class RecordStorageReader implements StorageReader
{
    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final TokenHolders tokenHolders;
    private final IndexingService indexService;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final PropertyStore propertyStore;
    private final CountsTracker counts;
    private final MetaDataStore metaDataStore;
    private final SchemaCache schemaCache;

    private boolean closed;

    RecordStorageReader(
            TokenHolders tokenHolders, NeoStores neoStores, CountsTracker counts,
            IndexingService indexService, SchemaCache schemaCache )
    {
        this.tokenHolders = tokenHolders;
        this.indexService = indexService;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.counts = counts;
        this.metaDataStore = neoStores.getMetaDataStore();
        this.schemaCache = schemaCache;
    }

    /**
     * All the nulls in this method is a testament to the fact that we probably need to break apart this reader,
     * separating index stuff out from store stuff.
     */
    public RecordStorageReader( NeoStores stores )
    {
        this( null, stores, null, null, null );
    }

    @Override
    public StorageIndexReference indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<StorageIndexReference> indexesGetForLabel( int labelId )
    {
        return map( descriptor -> descriptor, schemaCache.indexDescriptorsForLabel( labelId ) );
    }

    @Override
    public StorageIndexReference indexGetForName( String name )
    {
        return schemaCache.indexDescriptorForName( name );
    }

    @Override
    public Iterator<StorageIndexReference> indexesGetAll()
    {
        return map( descriptor -> descriptor, schemaCache.indexDescriptors().iterator() );
    }

    @Override
    public Iterator<StorageIndexReference> indexesGetRelatedToProperty( int propertyId )
    {
        return map( descriptor -> descriptor, schemaCache.indexesByProperty( propertyId ) );
    }

    @Override
    public long indexSize( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        Register.DoubleLongRegister result = indexService.indexUpdatesAndSize( descriptor );
        return result.readSecond();
    }

    @Override
    public double indexUniqueValuesPercentage( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.indexUniqueValuesPercentage( descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.constraintsForSchema( descriptor );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        return schemaCache.hasConstraintRule( descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        return schemaCache.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        return schemaCache.constraintsForRelationshipType( typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        return schemaCache.constraints();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( StorageIndexReference index )
    {
        if ( index instanceof StoreIndexDescriptor )
        {
            Long owningConstraint = ((StoreIndexDescriptor) index).getOwningConstraint();
            return schemaCache.hasConstraintRule( owningConstraint ) ? owningConstraint : null;
        }
        else
        {
            return null;
        }
    }

    @Override
    public long countsForNode( int labelId )
    {
        return counts.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        if ( !(startLabelId == StatementConstants.ANY_LABEL || endLabelId == StatementConstants.ANY_LABEL) )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        return counts.relationshipCount( startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long nodesGetCount()
    {
        return nodeStore.getNumberOfIdsInUse();
    }

    @Override
    public long relationshipsGetCount()
    {
        return relationshipStore.getNumberOfIdsInUse();
    }

    @Override
    public int labelCount()
    {
        return tokenHolders.labelTokens().size();
    }

    @Override
    public int propertyKeyCount()
    {
        return tokenHolders.propertyKeyTokens().size();
    }

    @Override
    public int relationshipTypeCount()
    {
        return tokenHolders.relationshipTypeTokens().size();
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return counts.indexUpdatesAndSize( tryGetIndexId( descriptor ), target );
    }

    @Override
    public DoubleLongRegister indexSample( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return counts.indexSample( tryGetIndexId( descriptor ), target );
    }

    private long tryGetIndexId( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexId( descriptor );
    }

    @Override
    public boolean nodeExists( long id )
    {
        return nodeStore.isInUse( id );
    }

    @Override
    public boolean relationshipExists( long id )
    {
        return relationshipStore.isInUse( id );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    @Override
    public AllNodeScan allNodeScan()
    {
        return new RecordNodeScan();
    }

    @Override
    public AllRelationshipsScan allRelationshipScan()
    {
        return new RecordRelationshipScan();
    }

    @Override
    public void close()
    {
        assert !closed;
        closed = true;
    }

    @Override
    public RecordNodeCursor allocateNodeCursor()
    {
        return new RecordNodeCursor( nodeStore );
    }

    @Override
    public StorageRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new RecordRelationshipGroupCursor( relationshipStore, relationshipGroupStore );
    }

    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new RecordRelationshipTraversalCursor( relationshipStore, relationshipGroupStore );
    }

    @Override
    public RecordRelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new RecordRelationshipScanCursor( relationshipStore, relationshipGroupStore );
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor()
    {
        return new RecordPropertyCursor( propertyStore, metaDataStore );
    }
}
