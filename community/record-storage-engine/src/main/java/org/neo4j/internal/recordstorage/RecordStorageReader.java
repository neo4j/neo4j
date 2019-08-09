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

import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.token.TokenHolders;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 */
public class RecordStorageReader implements StorageReader
{
    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final TokenHolders tokenHolders;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final PropertyStore propertyStore;
    private final CountsTracker counts;
    private final MetaDataStore metaDataStore;
    private final SchemaCache schemaCache;

    private boolean closed;

    RecordStorageReader( TokenHolders tokenHolders, NeoStores neoStores, CountsTracker counts, SchemaCache schemaCache )
    {
        this.tokenHolders = tokenHolders;
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
        this( null, stores, null, null );
    }

    @Override
    public IndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType( int relationshipType )
    {
        return schemaCache.indexDescriptorsForRelationshipType( relationshipType );
    }

    @Override
    public IndexDescriptor indexGetForName( String name )
    {
        return schemaCache.indexDescriptorForName( name );
    }

    @Override
    public ConstraintDescriptor constraintGetForName( String name )
    {
        return schemaCache.constraintForName( name );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return schemaCache.indexDescriptors().iterator();
    }

    @Override
    public Collection<SchemaDescriptor> indexesGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        return schemaCache.getIndexesRelatedTo( EMPTY_LONG_ARRAY, labels, new int[]{propertyKeyId}, false, entityType );
    }

    @Override
    public Collection<SchemaDescriptor> indexesGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        return schemaCache.getIndexesRelatedTo( labels, PrimitiveLongCollections.EMPTY_LONG_ARRAY, propertyKeyIds, true, entityType );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        return schemaCache.getUniquenessConstraintsRelatedTo( PrimitiveLongCollections.EMPTY_LONG_ARRAY, labels, new int[] {propertyKeyId}, false, entityType );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        return schemaCache.getUniquenessConstraintsRelatedTo( labels, PrimitiveLongCollections.EMPTY_LONG_ARRAY, propertyKeyIds, true, entityType );
    }

    @Override
    public boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
    {
        return schemaCache.hasRelatedSchema( labels, propertyKey, entityType );
    }

    @Override
    public boolean hasRelatedSchema( int label, EntityType entityType )
    {
        return schemaCache.hasRelatedSchema( label, entityType );
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
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        if ( index == null )
        {
            return null;
        }
        OptionalLong owningConstraintId = index.getOwningConstraintId();
        if ( owningConstraintId.isPresent() )
        {
            Long constraintId = owningConstraintId.getAsLong();
            if ( schemaCache.hasConstraintRule( constraintId ) )
            {
                return constraintId;
            }
        }
        return null;
    }

    @Override
    public long countsForNode( int labelId )
    {
        return counts.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        if ( !(startLabelId == ANY_LABEL || endLabelId == ANY_LABEL) )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        return counts.relationshipCount( startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long nodesGetCount()
    {
        if ( counts != null )
        {
            try
            {
                return counts.nodeCount( ANY_LABEL, newDoubleLongRegister() ).readSecond();
            }
            catch ( IllegalStateException e )
            {
                // This can happen if requesting nodes count before the store has been fully recovered.
                // Counts store cannot return values until then, so we'll just have to return an estimate.
                // The only use case here at the time of writing this is index population progress during recovery.
            }
        }
        // else this reader was instantiated as a simpler reader only over a NeoStores

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
    public StorageSchemaReader schemaSnapshot()
    {
        return new StorageSchemaReaderSnapshot( schemaCache.snapshot() );
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor()
    {
        return new RecordPropertyCursor( propertyStore, metaDataStore );
    }
}
