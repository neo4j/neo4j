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
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.CapableIndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.register.Registers.newDoubleLongRegister;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 */
public class RecordStorageReader implements StorageReader
{
    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTokenHolder;
    private final IndexingService indexService;
    private final NeoStores neoStores;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final PropertyStore propertyStore;
    private final SchemaStorage schemaStorage;
    private final CountsTracker counts;
    private final SchemaCache schemaCache;

    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private final Supplier<LabelScanReader> labelScanReaderSupplier;
    private final RecordCursors recordCursors;
    private final RecordStorageCommandCreationContext commandCreationContext;

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;

    RecordStorageReader( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage, NeoStores neoStores,
            IndexingService indexService, SchemaCache schemaCache,
            Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier,
            RecordStorageCommandCreationContext commandCreationContext )
    {
        this.neoStores = neoStores;
        this.relationshipTokenHolder = relationshipTokenHolder;
        this.schemaStorage = schemaStorage;
        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.counts = neoStores.getCounts();
        this.schemaCache = schemaCache;
        this.indexReaderFactorySupplier = indexReaderFactory;
        this.labelScanReaderSupplier = labelScanReaderSupplier;
        this.commandCreationContext = commandCreationContext;
        this.recordCursors = new RecordCursors( neoStores );
    }

    /**
     * All the nulls in this method is a testament to the fact that we probably need to break apart this reader,
     * separating index stuff out from store stuff.
     */
    public static RecordStorageReader newReader( NeoStores stores )
    {
        return new RecordStorageReader( null, null, null, null, stores, null, null, null, null, null );
    }

    @Override
    public int labelGetOrCreateForName( String label ) throws TooManyLabelsException
    {
        try
        {
            return labelTokenHolder.getOrCreateId( label );
        }
        catch ( TransactionFailureException e )
        {
            // Temporary workaround for the property store based label
            // implementation. Actual
            // implementation should not depend on internal kernel exception
            // messages like this.
            if ( e.getCause() instanceof UnderlyingStorageException &&
                    e.getCause().getMessage().equals( "Id capacity exceeded" ) )
            {
                throw new TooManyLabelsException( e );
            }
            throw e;
        }
    }

    @Override
    public void labelGetOrCreateForNames( String[] labelNames, int[] labelIds ) throws TooManyLabelsException
    {
        try
        {
            labelTokenHolder.getOrCreateIds( labelNames, labelIds );
        }
        catch ( TransactionFailureException e )
        {
            // Temporary workaround for the property store based label implementation.
            // Actual implementation should not depend on internal kernel exception messages like this.
            if ( e.getCause() instanceof UnderlyingStorageException &&
                    e.getCause().getMessage().equals( "Id capacity exceeded" ) )
            {
                throw new TooManyLabelsException( e );
            }
            throw e;
        }
    }

    @Override
    public int labelGetForName( String label )
    {
        return labelTokenHolder.getIdByName( label );
    }

    @Override
    public String labelGetName( long labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokenHolder.getTokenById( toIntExact( labelId ) ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( labelId, e );
        }
    }

    @Override
    public PrimitiveLongResourceIterator nodesGetForLabel( int labelId )
    {
        return getLabelScanReader().nodesWithLabel( labelId );
    }

    @Override
    public CapableIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetAll()
    {
        return schemaCache.indexDescriptors().iterator();
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        StoreIndexDescriptor storeIndexDescriptor = getStoreIndexDescriptor( index );
        if ( storeIndexDescriptor != null )
        {
            // Think of the index as being orphaned if the owning constraint is missing or broken.
            Long owningConstraint = storeIndexDescriptor.getOwningConstraint();
            return schemaCache.hasConstraintRule( owningConstraint ) ? owningConstraint : null;
        }
        return null;
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getState();
    }

    public IndexReference indexReference( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        IndexProxy indexProxy = indexService.getIndexProxy( descriptor.schema() );
        return indexProxy.getDescriptor();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getIndexPopulationProgress();
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
    public String indexGetFailure( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getPopulationFailure().asString();
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
    public int propertyKeyGetOrCreateForName( String propertyKey )
    {
        return propertyKeyTokenHolder.getOrCreateId( propertyKey );
    }

    @Override
    public void propertyKeyGetOrCreateForNames( String[] propertyKeys, int[] ids )
    {
        propertyKeyTokenHolder.getOrCreateIds( propertyKeys, ids );
    }

    @Override
    public int propertyKeyGetForName( String propertyKey )
    {
        return propertyKeyTokenHolder.getIdByName( propertyKey );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        try
        {
            return propertyKeyTokenHolder.getTokenById( propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundKernelException( propertyKeyId, e );
        }
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        return propertyKeyTokenHolder.getAllTokens().iterator();
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        return labelTokenHolder.getAllTokens().iterator();
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public Iterator<Token> relationshipTypeGetAllTokens()
    {
        return (Iterator) relationshipTokenHolder.getAllTokens().iterator();
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        return relationshipTokenHolder.getIdByName( relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        try
        {
            return relationshipTokenHolder.getTokenById( relationshipTypeId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new RelationshipTypeIdNotFoundKernelException( relationshipTypeId, e );
        }
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName )
    {
        return relationshipTokenHolder.getOrCreateId( relationshipTypeName );
    }

    @Override
    public void relationshipTypeGetOrCreateForNames( String[] relationshipTypeNames, int[] ids )
    {
        relationshipTokenHolder.getOrCreateIds( relationshipTypeNames, ids );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> relationshipVisitor ) throws EntityNotFoundException, EXCEPTION
    {
        // TODO Please don't create a record for this, it's ridiculous
        RelationshipRecord record = relationshipStore.getRecord( relationshipId, relationshipStore.newRecord(), CHECK );
        if ( !record.inUse() )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        relationshipVisitor.visit( relationshipId, record.getType(), record.getFirstNode(), record.getSecondNode() );
    }

    @Override
    public void releaseNode( long id )
    {
        nodeStore.freeId( id );
    }

    @Override
    public void releaseRelationship( long id )
    {
        relationshipStore.freeId( id );
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
        return labelTokenHolder.size();
    }

    @Override
    public int propertyKeyCount()
    {
        return propertyKeyTokenHolder.size();
    }

    @Override
    public int relationshipTypeCount()
    {
        return relationshipTokenHolder.size();
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

    private StoreIndexDescriptor getStoreIndexDescriptor( IndexDescriptor index )
    {
        for ( StoreIndexDescriptor descriptor : schemaCache.indexDescriptors() )
        {
            if ( descriptor.equals( index ) )
            {
                return descriptor;
            }
        }

        return schemaStorage.indexGetForSchema( index );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    @Override
    public void acquire()
    {
        assert !closed;
        assert !acquired;
        this.acquired = true;
    }

    @Override
    public void release()
    {
        assert !closed;
        assert acquired;
        closeSchemaResources();
        acquired = false;
    }

    @Override
    public void close()
    {
        assert !closed;
        closeSchemaResources();
        recordCursors.close();
        commandCreationContext.close();
        closed = true;
    }

    private void closeSchemaResources()
    {
        if ( indexReaderFactory != null )
        {
            indexReaderFactory.close();
            // we can actually keep this object around
        }
        if ( labelScanReader != null )
        {
            labelScanReader.close();
            labelScanReader = null;
        }
    }

    @Override
    public LabelScanReader getLabelScanReader()
    {
        return labelScanReader != null ?
               labelScanReader : (labelScanReader = labelScanReaderSupplier.get());
    }

    private IndexReaderFactory indexReaderFactory()
    {
        return indexReaderFactory != null ?
               indexReaderFactory : (indexReaderFactory = indexReaderFactorySupplier.get());
    }

    @Override
    public IndexReader getIndexReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newReader( descriptor );
    }

    @Override
    public IndexReader getFreshIndexReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newUnCachedReader( descriptor );
    }

    RecordStorageCommandCreationContext getCommandCreationContext()
    {
        return commandCreationContext;
    }

    @Override
    public long reserveNode()
    {
        return commandCreationContext.nextId( StoreType.NODE );
    }

    @Override
    public long reserveRelationship()
    {
        return commandCreationContext.nextId( StoreType.RELATIONSHIP );
    }

    @Override
    public long getGraphPropertyReference()
    {
        return neoStores.getMetaDataStore().getGraphNextProp();
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
        return new RecordPropertyCursor( propertyStore );
    }
}
