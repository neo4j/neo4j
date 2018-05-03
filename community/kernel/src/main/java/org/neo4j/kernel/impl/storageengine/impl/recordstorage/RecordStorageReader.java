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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.toDescriptor;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.INCOMING;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.LOOP;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.OUTGOING;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.DegreeCounter.countByFirstPrevPointer;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.DegreeCounter.countRelationshipsInGroup;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.GroupReferenceEncoding.isRelationship;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 */
class RecordStorageReader extends DefaultCursors implements StorageReader, TxStateHolder
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
    private final PropertyLoader propertyLoader;
    private final SchemaCache schemaCache;

    // State from the old StoreStatement
    private final InstanceCache<StoreSingleNodeCursor> singleNodeCursor;
    private final InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private final InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;
    private final InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipsCursor;
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;

    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private final Supplier<LabelScanReader> labelScanReaderSupplier;
    private final RecordCursors recordCursors;
    private final RecordStorageCommandCreationContext commandCreationContext;

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;
    private TxStateHolder txStateHolder;
    private AccessMode accessMode;
    private AssertOpen assertOpen;

    RecordStorageReader( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage, NeoStores neoStores,
            IndexingService indexService, SchemaCache schemaCache,
            Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier, LockService lockService,
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
        this.propertyLoader = new PropertyLoader( neoStores );
        this.schemaCache = schemaCache;
        this.indexReaderFactorySupplier = indexReaderFactory;
        this.labelScanReaderSupplier = labelScanReaderSupplier;
        this.commandCreationContext = commandCreationContext;

        this.recordCursors = new RecordCursors( neoStores );
        this.singleNodeCursor = new InstanceCache<StoreSingleNodeCursor>()
        {
            @Override
            protected StoreSingleNodeCursor create()
            {
                return new StoreSingleNodeCursor( nodeStore.newRecord(), this, recordCursors, lockService );
            }
        };
        this.singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( relationshipStore.newRecord(), this, recordCursors,
                        lockService );
            }
        };
        this.iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( relationshipStore.newRecord(), this, recordCursors,
                        lockService );
            }
        };
        this.nodeRelationshipsCursor = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( relationshipStore.newRecord(),
                        relationshipGroupStore.newRecord(), this, recordCursors, lockService );
            }
        };

        this.singlePropertyCursorCache = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( recordCursors, this );
            }
        };
        this.propertyCursorCache = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( recordCursors, this );
            }
        };
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
    public int labelGetForName( String label )
    {
        return labelTokenHolder.getIdByName( label );
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokenHolder.getTokenById( labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( "Label by id " + labelId, e );
        }
    }

    @Override
    public PrimitiveLongResourceIterator nodesGetForLabel( int labelId )
    {
        return getLabelScanReader().nodesWithLabel( labelId );
    }

    @Override
    public SchemaIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<SchemaIndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( SchemaIndexDescriptor index )
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            // Think of the index as being orphaned if the owning constraint is missing or broken.
            Long owningConstraint = rule.getOwningConstraint();
            return schemaCache.hasConstraintRule( owningConstraint ) ? owningConstraint : null;
        }
        return null;
    }

    @Override
    public long indexGetCommittedId( SchemaIndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule == null )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, index.schema() );
        }
        return rule.getId();
    }

    @Override
    public InternalIndexState indexGetState( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return getIndexProxy( descriptor ).getState();
    }

    @Override
    public IndexProvider.Descriptor indexGetProviderDescriptor( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return getIndexProxy( descriptor ).getProviderDescriptor();
    }

    public CapableIndexReference indexReference( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        boolean unique = descriptor.type() == SchemaIndexDescriptor.Type.UNIQUE;
        SchemaDescriptor schema = descriptor.schema();
        IndexProxy indexProxy = indexService.getIndexProxy( schema );

        return new DefaultCapableIndexReference( unique, indexProxy.getIndexCapability(),
                indexProxy.getProviderDescriptor(), schema.keyId(),
                schema.getPropertyIds() );
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
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        return null;
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
    public IntIterator graphGetPropertyKeys()
    {
        return new PropertyKeyIdIterator( propertyLoader.graphLoadProperties( new IteratingPropertyReceiver<>() ) );
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<StorageProperty> graphGetAllProperties()
    {
        return propertyLoader.graphLoadProperties( new IteratingPropertyReceiver<>() );
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
    public LongIterator nodesGetAll()
    {
        return new AllNodeIterator( nodeStore );
    }

    @Override
    public RelationshipIterator relationshipsGetAll()
    {
        return new AllRelationshipIterator( relationshipStore );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( NodeItem nodeItem,
            Direction direction )
    {
        return nodeGetRelationships( nodeItem, direction, ALWAYS_TRUE_INT );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( NodeItem node,
            Direction direction, IntPredicate relTypes )
    {
        return acquireNodeRelationshipCursor( node.isDense(), node.id(), node.nextRelationshipId(), direction,
                relTypes );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( NodeItem node, AssertOpen assertOpen )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return acquirePropertyCursor( node.nextPropertyId(), lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperty( NodeItem node, int propertyKeyId,
            AssertOpen assertOpen )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return acquireSinglePropertyCursor( node.nextPropertyId(), propertyKeyId, lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( RelationshipItem relationship,
            AssertOpen assertOpen )
    {
        Lock lock = relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return acquirePropertyCursor( relationship.nextPropertyId(), lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperty( RelationshipItem relationship,
            int propertyKeyId, AssertOpen assertOpen )
    {
        Lock lock = relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return acquireSinglePropertyCursor( relationship.nextPropertyId(), propertyKeyId, lock, assertOpen );
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
    public long countsForNodeWithoutTxState( int labelId )
    {
        return 0;
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
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        return 0;
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
    public DoubleLongRegister indexUpdatesAndSize( IndexReference index, DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return counts.indexUpdatesAndSize( tryGetIndexId( toDescriptor( index ).schema() ), target );
    }

    @Override
    public DoubleLongRegister indexSample( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return counts.indexSample( tryGetIndexId( descriptor ), target );
    }

    @Override
    public DoubleLongRegister indexSample( IndexReference index, DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return counts.indexSample( tryGetIndexId( toDescriptor( index ).schema() ), target );
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
    public IntSet relationshipTypes( NodeItem node )
    {
        final MutableIntSet set = new IntHashSet();
        if ( node.isDense() )
        {
            RelationshipGroupRecord groupRecord = relationshipGroupStore.newRecord();
            RecordCursor<RelationshipGroupRecord> cursor = recordCursors().relationshipGroup();
            for ( long id = node.nextGroupId(); id != NO_NEXT_RELATIONSHIP.intValue(); id = groupRecord.getNext() )
            {
                if ( cursor.next( id, groupRecord, FORCE ) )
                {
                    set.add( groupRecord.getType() );
                }
            }
        }
        else
        {
            nodeGetRelationships( node, Direction.BOTH )
                    .forAll( relationship -> set.add( relationship.type() ) );
        }
        return set;
    }

    @Override
    public void degrees( NodeItem nodeItem, DegreeVisitor visitor )
    {
        if ( nodeItem.isDense() )
        {
            visitDenseNode( nodeItem, visitor );
        }
        else
        {
            visitNode( nodeItem, visitor );
        }
    }

    private IndexRule indexRule( SchemaIndexDescriptor index )
    {
        for ( IndexRule rule : schemaCache.indexRules() )
        {
            if ( rule.getIndexDescriptor().equals( index ) )
            {
                return rule;
            }
        }

        return schemaStorage.indexGetForSchema( index );
    }

    @Override
    public int degreeRelationshipsInGroup( long nodeId, long groupId,
            Direction direction, Integer relType )
    {
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        RelationshipGroupRecord relationshipGroupRecord = relationshipGroupStore.newRecord();
        return countRelationshipsInGroup( groupId, direction, relType, nodeId, relationshipRecord,
                relationshipGroupRecord, recordCursors() );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    private void visitNode( NodeItem nodeItem, DegreeVisitor visitor )
    {
        try ( Cursor<RelationshipItem> relationships = nodeGetRelationships( nodeItem, Direction.BOTH ) )
        {
            while ( relationships.next() )
            {
                RelationshipItem rel = relationships.get();
                int type = rel.type();
                switch ( directionOf( nodeItem.id(), rel.id(), rel.startNode(), rel.endNode() ) )
                {
                case OUTGOING:
                    visitor.visitDegree( type, 1, 0 );
                    break;
                case INCOMING:
                    visitor.visitDegree( type, 0, 1 );
                    break;
                case BOTH:
                    visitor.visitDegree( type, 1, 1 );
                    break;
                default:
                    throw new IllegalStateException( "You found the missing direction!" );
                }
            }
        }
    }

    private void visitDenseNode( NodeItem nodeItem, DegreeVisitor visitor )
    {
        RelationshipGroupRecord relationshipGroupRecord = relationshipGroupStore.newRecord();
        RecordCursor<RelationshipGroupRecord> relationshipGroupCursor = recordCursors().relationshipGroup();
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        RecordCursor<RelationshipRecord> relationshipCursor = recordCursors().relationship();

        long groupId = nodeItem.nextGroupId();
        while ( groupId != NO_NEXT_RELATIONSHIP.longValue() )
        {
            relationshipGroupCursor.next( groupId, relationshipGroupRecord, FORCE );
            if ( relationshipGroupRecord.inUse() )
            {
                int type = relationshipGroupRecord.getType();

                long firstLoop = relationshipGroupRecord.getFirstLoop();
                long firstOut = relationshipGroupRecord.getFirstOut();
                long firstIn = relationshipGroupRecord.getFirstIn();

                long loop = countByFirstPrevPointer( firstLoop, relationshipCursor, nodeItem.id(), relationshipRecord );
                long outgoing =
                        countByFirstPrevPointer( firstOut, relationshipCursor, nodeItem.id(), relationshipRecord ) +
                                loop;
                long incoming =
                        countByFirstPrevPointer( firstIn, relationshipCursor, nodeItem.id(), relationshipRecord ) +
                                loop;
                visitor.visitDegree( type, outgoing, incoming );
            }
            groupId = relationshipGroupRecord.getNext();
        }
    }

    private Direction directionOf( long nodeId, long relationshipId, long startNode, long endNode )
    {
        if ( startNode == nodeId )
        {
            return endNode == nodeId ? Direction.BOTH : Direction.OUTGOING;
        }
        if ( endNode == nodeId )
        {
            return Direction.INCOMING;
        }
        throw new InvalidRecordException(
                "Node " + nodeId + " neither start nor end node of relationship " + relationshipId +
                        " with startNode:" + startNode + " and endNode:" + endNode );
    }

    private static Iterator<SchemaIndexDescriptor> toIndexDescriptors( Iterable<IndexRule> rules )
    {
        return Iterators.map( IndexRule::getIndexDescriptor, rules.iterator() );
    }

    @Override
    public void beginTransaction( TxStateHolder txStateHolder, AccessMode accessMode, AssertOpen assertOpen )
    {
        this.txStateHolder = txStateHolder;
        this.accessMode = accessMode;
        this.assertOpen = assertOpen;
    }

    @Override
    public void beginStatement()
    {
        assert !closed;
        assert !acquired;
        this.acquired = true;
    }

    @Override
    public void endStatement()
    {
        assert !closed;
        assert acquired;
        closeSchemaResources();
        acquired = false;
    }

    @Override
    public void endTransaction()
    {
        assert !closed;
        assert !acquired;
        closeSchemaResources();
        release(); // the new cursors
    }

    @Override
    public void close()
    {
        assert !closed;
        assert !acquired;
        closeSchemaResources();
        recordCursors.close(); // the old cursors
        commandCreationContext.close();
        closed = true;
    }

    @Override
    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId )
    {
        neoStores.assertOpen();
        return singleNodeCursor.get().init( nodeId );
    }

    @Override
    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relId )
    {
        neoStores.assertOpen();
        return singleRelationshipCursor.get().init( relId );
    }

    @Override
    public Cursor<RelationshipItem> acquireNodeRelationshipCursor( boolean isDense, long nodeId, long relationshipId,
            Direction direction, IntPredicate relTypeFilter )
    {
        neoStores.assertOpen();
        return nodeRelationshipsCursor.get().init( isDense, relationshipId, nodeId, direction, relTypeFilter );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor()
    {
        neoStores.assertOpen();
        return iteratorRelationshipCursor.get().init( new AllIdIterator( relationshipStore ) );
    }

    @Override
    public Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock lock, AssertOpen assertOpen )
    {
        return propertyCursorCache.get().init( propertyId, lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock lock,
            AssertOpen assertOpen )
    {
        return singlePropertyCursorCache.get().init( propertyId, propertyKeyId, lock, assertOpen );
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
    public IndexReader getIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newReader( descriptor );
    }

    @Override
    public IndexReader getFreshIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newUnCachedReader( descriptor );
    }

    @Override
    public RecordCursors recordCursors()
    {
        return recordCursors;
    }

    public RecordStorageCommandCreationContext getCommandCreationContext()
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
    public final void nodeIndexSeek(
            IndexReference index,
            NodeValueIndexCursor cursor,
            IndexOrder indexOrder,
            IndexQuery... query ) throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        if ( hasForbiddenProperties( index ) )
        {
            cursor.close();
            return;
        }

        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( index, false );
        cursorImpl.setRead( this, null );
        IndexProgressor.NodeValueClient target = withFullValuePrecision( cursorImpl, query, reader );
        reader.query( target, indexOrder, query );
    }

    private IndexProgressor.NodeValueClient withFullValuePrecision( DefaultNodeValueIndexCursor cursor,
            IndexQuery[] query, IndexReader reader )
    {
        IndexProgressor.NodeValueClient target = cursor;
        if ( !reader.hasFullValuePrecision( query ) )
        {
            IndexQuery[] filters = new IndexQuery[query.length];
            int j = 0;
            for ( IndexQuery q : query )
            {
                switch ( q.type() )
                {
                case range:
                    ValueGroup valueGroup = q.valueGroup();
                    if ( ( valueGroup == NUMBER || valueGroup == GEOMETRY) && !reader.hasFullValuePrecision( q ) )
                    {
                        filters[j++] = q;
                    }
                    break;
                case exact:
                    Value value = ((IndexQuery.ExactPredicate) q).value();
                    if ( value.valueGroup() == ValueGroup.NUMBER || Values.isArrayValue( value ) || value.valueGroup() == ValueGroup.GEOMETRY )
                    {
                        if ( !reader.hasFullValuePrecision( q ) )
                        {
                            filters[j++] = q;
                        }
                    }
                    break;
                default:
                    break;
                }
            }
            if ( j > 0 )
            {
                filters = Arrays.copyOf( filters, j );
                target = new NodeValueClientFilter( target, allocateNodeCursor(),
                        allocatePropertyCursor(), this, filters );
            }
        }
        return target;
    }

    @Override
    public final long lockingNodeUniqueIndexSeek(
            IndexReference index,
            IndexQuery.ExactPredicate... predicates )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException
    {
        IndexReader reader = indexReader( index, true );
        // TODO don't instantiate here hard-coded
        DefaultNodeValueIndexCursor cursor = new DefaultNodeValueIndexCursor( null );
        cursor.setRead( this, reader );
        IndexProgressor.NodeValueClient target = withFullValuePrecision( cursor, predicates, reader );
        reader.query( target, IndexOrder.NONE, predicates );
        return cursor.next() ? cursor.nodeReference() : NO_ID;
    }

    @Override
    public final void nodeIndexScan(
            IndexReference index,
            NodeValueIndexCursor cursor,
            IndexOrder indexOrder ) throws KernelException
    {
        if ( hasForbiddenProperties( index ) )
        {
            cursor.close();
            return;
        }

        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = index.properties()[0];
        ((DefaultNodeValueIndexCursor) cursor).setRead( this, null );
        indexReader( index, false ).query( (DefaultNodeValueIndexCursor) cursor, indexOrder, IndexQuery.exists( firstProperty ) );
    }

    private boolean hasForbiddenProperties( IndexReference index )
    {
        for ( int prop : index.properties() )
        {
            if ( !accessMode.allowsPropertyReads( prop ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( this );
        labelScanReader().nodesWithLabel( indexCursor, label);
    }

    private LabelScanReader labelScanReader()
    {
        if ( labelScanReader == null )
        {
            labelScanReader = labelScanReaderSupplier.get();
        }
        return labelScanReader;
    }

    @Override
    public void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        DefaultNodeLabelIndexCursor client = (DefaultNodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.unionScan( new NodeLabelIndexProgressor( labelScanReader().nodesWithAnyOfLabels( labels ), client ),
                false, labels );
    }

    @Override
    public void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        DefaultNodeLabelIndexCursor client = (DefaultNodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.intersectionScan(
                new NodeLabelIndexProgressor( labelScanReader().nodesWithAllLabels( labels ), client ),
                false, labels );
    }

    @Override
    public final Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void allNodesScan( NodeCursor cursor )
    {
        ((DefaultNodeCursor) cursor).scan( this );
    }

    @Override
    public final Scan<NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void singleNode( long reference, NodeCursor cursor )
    {
        ((DefaultNodeCursor) cursor).single( reference, this );
    }

    @Override
    public final void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).single( reference, this );
    }

    @Override
    public final void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).scan( -1/*include all labels*/, this );
    }

    @Override
    public final Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipTypeScan( int type, RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).scan( type, this );
    }

    @Override
    public final Scan<RelationshipScanCursor> relationshipTypeScan( int type )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipGroups(
            long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        // the relationships for this node are not grouped in the store
        if ( reference != NO_ID && isRelationship( reference ) )
        {
            ((DefaultRelationshipGroupCursor) cursor).buffer( nodeReference, clearEncoding( reference ), this );
        }
        else // this is a normal group reference.
        {
            ((DefaultRelationshipGroupCursor) cursor).direct( nodeReference, reference, this );
        }
    }

    @Override
    public final void relationships(
            long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        /* There are 5 different ways a relationship traversal cursor can be initialized:
         *
         * 1. From a batched group in a detached way. This happens when the user manually retrieves the relationships
         *    references from the group cursor and passes it to this method and if the group cursor was based on having
         *    batched all the different types in the single (mixed) chain of relationships.
         *    In this case we should pass a reference marked with some flag to the first relationship in the chain that
         *    has the type of the current group in the group cursor. The traversal cursor then needs to read the type
         *    from that first record and use that type as a filter for when reading the rest of the chain.
         *    - NOTE: we probably have to do the same sort of filtering for direction - so we need a flag for that too.
         *
         * 2. From a batched group in a DIRECT way. This happens when the traversal cursor is initialized directly from
         *    the group cursor, in this case we can simply initialize the traversal cursor with the buffered state from
         *    the group cursor, so this method here does not have to be involved, and things become pretty simple.
         *
         * 3. Traversing all relationships - regardless of type - of a node that has grouped relationships. In this case
         *    the traversal cursor needs to traverse through the group records in order to get to the actual
         *    relationships. The initialization of the cursor (through this here method) should be with a FLAGGED
         *    reference to the (first) group record.
         *
         * 4. Traversing a single chain - this is what happens in the cases when
         *    a) Traversing all relationships of a node without grouped relationships.
         *    b) Traversing the relationships of a particular group of a node with grouped relationships.
         *
         * 5. There are no relationships - i.e. passing in NO_ID to this method.
         *
         * This means that we need reference encodings (flags) for cases: 1, 3, 4, 5
         */
        int relationshipType;
        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        DefaultRelationshipTraversalCursor internalCursor = (DefaultRelationshipTraversalCursor)cursor;

        switch ( encoding )
        {
        case NONE: // this is a normal relationship reference
            internalCursor.chain( nodeReference, reference, this );
            break;

        case FILTER: // this relationship chain needs to be filtered
            internalCursor.filtered( nodeReference, clearEncoding( reference ), this, true );
            break;

        case FILTER_TX_STATE: // tx-state changes should be filtered by the head of this chain
            internalCursor.filtered( nodeReference, clearEncoding( reference ), this, false );
            break;

        case GROUP: // this reference is actually to a group record
            internalCursor.groups( nodeReference, clearEncoding( reference ), this );
            break;

        case NO_OUTGOING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, OUTGOING );
            break;

        case NO_INCOMING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, INCOMING );
            break;

        case NO_LOOP_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, LOOP );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }
    }

    @Override
    public final void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference, reference, this );
    }

    @Override
    public final void relationshipProperties( long relationshipReference, long reference,
            PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference, reference, this );
    }

    @Override
    public final void graphProperties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initGraph( graphPropertiesReference(), this );
    }

    private long graphPropertiesReference()
    {
        return neoStores.getMetaDataStore().getGraphNextProp();
    }

    @Override
    public final void futureNodeReferenceRead( long reference )
    {
    }

    @Override
    public final void futureRelationshipsReferenceRead( long reference )
    {
    }

    @Override
    public final void futureNodePropertyReferenceRead( long reference )
    {
    }

    @Override
    public final void futureRelationshipPropertyReferenceRead( long reference )
    {
    }

    private IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException
    {
        SchemaIndexDescriptor schemaIndexDescriptor = toDescriptor( index );
        return fresh ? getFreshIndexReader( schemaIndexDescriptor ) : getIndexReader( schemaIndexDescriptor );
    }

    PageCursor nodePage( long reference )
    {
        return nodeStore.openPageCursorForReading( reference );
    }

    PageCursor relationshipPage( long reference )
    {
        return relationshipStore.openPageCursorForReading( reference );
    }

    PageCursor groupPage( long reference )
    {
        return relationshipGroupStore.openPageCursorForReading( reference );
    }

    PageCursor propertyPage( long reference )
    {
        return propertyStore.openPageCursorForReading( reference );
    }

    PageCursor stringPage( long reference )
    {
        return propertyStore.openStringPageCursor( reference );
    }

    PageCursor arrayPage( long reference )
    {
        return propertyStore.openArrayPageCursor( reference );
    }

    RecordCursor<DynamicRecord> labelCursor()
    {
        return nodeStore.newLabelCursor();
    }

    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        nodeStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully here since otherwise we can have inconsistent reads
        // for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
        propertyStore.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        // We need to load forcefully here since otherwise we cannot traverse over groups
        // records which have been concurrently deleted (flagged as inUse = false).
        // @see #org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        relationshipGroupStore.getRecordByCursor( reference, record, RecordLoad.FORCE, page );
    }

    TextValue string( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = propertyStore.loadString( reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    ArrayValue array( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = propertyStore.loadArray( reference, cursor.buffer, page );
        buffer.flip();
        return PropertyStore.readArrayFromBuffer( buffer );
    }

    AccessMode accessMode()
    {
        return accessMode;
    }

    long nodeHighMark()
    {
        return nodeStore.getHighestPossibleIdInUse();
    }

    long relationshipHighMark()
    {
        return relationshipStore.getHighestPossibleIdInUse();
    }

    void assertOpen()
    {
        assertOpen.assertOpen();
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        return asIndexReferences( schemaCache.indexDescriptorsForLabel( labelId ) );
    }

    private Iterator<IndexReference> asIndexReferences( Iterator<SchemaIndexDescriptor> descriptors )
    {
        return Iterators.map( DefaultIndexReference::fromDescriptor, descriptors );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        return Iterators.map( rule -> DefaultIndexReference.fromDescriptor( rule.getIndexDescriptor() ), schemaCache.indexRules().iterator() );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        return indexGetState( toDescriptor( index ) );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        return getIndexProxy( toDescriptor( index ) ).getIndexPopulationProgress();
    }

    private IndexProxy getIndexProxy( SchemaIndexDescriptor toDescriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( toDescriptor.schema() );
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaRuleNotFoundException
    {
        SchemaIndexDescriptor descriptor = toDescriptor( index );
        IndexRule rule = indexRule( descriptor );
        if ( rule == null )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, descriptor.schema() );
        }
        return rule.getId();
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        return getIndexProxy( toDescriptor( index ) ).getPopulationFailure().asString();
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        return indexService.indexUniqueValuesPercentage( toDescriptor( index ).schema() );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        Register.DoubleLongRegister result = indexService.indexUpdatesAndSize( toDescriptor( index ).schema() );
        return result.readSecond();
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, Value value ) throws KernelException
    {
        IndexReader reader = getIndexReader( toDescriptor( index ) );
        return reader.countIndexedNodes( nodeId, value );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K,V> creator )
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public <K, V> V schemaStateGet( K key )
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public void schemaStateFlush()
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public TransactionState txState()
    {
        return txStateHolder.txState();
    }

    @Override
    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return txStateHolder.explicitIndexTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txStateHolder.hasTxStateWithChanges();
    }

    private ExplicitIndex explicitNodeIndex( String index ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexTxState().nodeChanges( index );
    }

    private ExplicitIndex explicitRelationshipIndex( String index ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexTxState().relationshipChanges( index );
    }

    private static void explicitIndex( IndexProgressor.ExplicitClient client, ExplicitIndexHits hits )
    {
        client.initialize( new ExplicitIndexProgressor( hits, client ), hits.size() );
    }

    @Override
    public void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Object value ) throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).get( key, value ) );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query ) throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query ) throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                key, query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Node, indexName, customConfiguration );
    }

    @Override
    public Map<String,String> nodeExplicitIndexGetConfiguration( String indexName )
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public void relationshipExplicitIndexLookup( RelationshipExplicitIndexCursor cursor, String index, String key, Object value, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultRelationshipExplicitIndexCursor) cursor, explicitRelationshipIndex( index ).get( key, value, source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultRelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index ).query( query instanceof Value ? ((Value) query).asObject() : query, source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, String key, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultRelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index ).query( key, query instanceof Value ? ((Value) query).asObject() : query, source, target ) );
    }

    @Override
    public boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Relationship, indexName, customConfiguration );
    }

    @Override
    public String[] nodeExplicitIndexesGetAll()
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll()
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }

    @Override
    public Map<String,String> relationshipExplicitIndexGetConfiguration( String indexName )
    {
        throw new UnsupportedOperationException( "Implemented on a higher level instead" );
    }
}
