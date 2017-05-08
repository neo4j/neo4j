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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.BatchingLongProgression;
import org.neo4j.storageengine.api.BatchingProgressionFactory;
import org.neo4j.storageengine.api.CursorPools;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.SchemaResources;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.NodeTransactionStateView;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.collection.primitive.Primitive.intSet;
import static org.neo4j.kernel.impl.util.Cursors.count;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.Direction.BOTH;
import static org.neo4j.storageengine.api.Direction.INCOMING;
import static org.neo4j.storageengine.api.Direction.OUTGOING;
import static org.neo4j.storageengine.api.txstate.ReadableTransactionState.EMPTY;

/**
 * Default implementation of StoreReadLayer. Delegates to NeoStores and indexes.
 */
public class StorageLayer implements StoreReadLayer
{
    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTokenHolder;
    private final IndexingService indexService;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final SchemaStorage schemaStorage;
    private final CountsTracker counts;
    private final PropertyLoader propertyLoader;
    private final Supplier<SchemaResources> schemaResourcesSupplier;
    private final SchemaCache schemaCache;
    private final CursorPools cursorPools;
    private final BatchingProgressionFactory progressionFactory;

    public StorageLayer( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage, NeoStores neoStores,
            IndexingService indexService, Supplier<SchemaResources> schemaResourcesSupplier, SchemaCache schemaCache,
            CursorPools cursorPools, BatchingProgressionFactory progressionFactory )
    {
        this.relationshipTokenHolder = relationshipTokenHolder;
        this.schemaStorage = schemaStorage;
        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.schemaResourcesSupplier = schemaResourcesSupplier;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.counts = neoStores.getCounts();
        this.propertyLoader = new PropertyLoader( neoStores );
        this.schemaCache = schemaCache;
        this.cursorPools = cursorPools;
        this.progressionFactory = progressionFactory;
    }

    @Override
    public SchemaResources schemaResources()
    {
        return schemaResourcesSupplier.get();
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
    public PrimitiveLongIterator nodesGetForLabel( SchemaResources statement, int labelId )
    {
        return statement.getLabelScanReader().nodesWithLabel( labelId );
    }

    @Override
    public IndexDescriptor indexGetForSchema( LabelSchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return toIndexDescriptors( schemaCache.indexRules() );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return null;
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index )
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
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getState();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( LabelSchemaDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getIndexPopulationProgress();
    }

    @Override
    public long indexSize( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        Register.DoubleLongRegister result = indexService.indexUpdatesAndSize( descriptor );
        return result.readSecond();
    }

    @Override
    public double indexUniqueValuesPercentage( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.indexUniqueValuesPercentage( descriptor );
    }

    @Override
    public String indexGetFailure( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getPopulationFailure().asString();
    }

    @Override
    public IndexReader indexGetReader( SchemaResources schemaResources, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        return schemaResources.getIndexReader( index );
    }

    @Override
    public IndexReader indexGetFreshReader( SchemaResources schemaResources, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        return schemaResources.getFreshIndexReader( index );
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
    public PrimitiveIntIterator graphGetPropertyKeys()
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
    public PrimitiveLongIterator nodesGetAll()
    {
        return new AllNodeIterator( nodeStore );
    }

    @Override
    public RelationshipIterator relationshipsGetAll()
    {
        return new AllRelationshipIterator( relationshipStore );
    }

    @Override
    public BatchingLongProgression parallelNodeScanProgression()
    {
        return progressionFactory.parallelAllNodeScan( nodeStore );
    }

    @Override
    public Cursor<NodeItem> nodeGetCursor( BatchingLongProgression progression, NodeTransactionStateView stateView )
    {
        return cursorPools.acquireNodeCursor( progression, stateView );
    }

    @Override
    public Cursor<NodeItem> nodeGetAllCursor( NodeTransactionStateView stateView )
    {
        return cursorPools.acquireNodeCursor( progressionFactory.allNodeScan( nodeStore ), stateView );
    }

    @Override
    public Cursor<NodeItem> nodeGetSingleCursor( long nodeId, NodeTransactionStateView stateView )
    {
        return cursorPools.acquireNodeCursor( progressionFactory.singleNodeFetch( nodeId ), stateView );
    }

    @Override
    public Cursor<RelationshipItem> relationshipGetSingleCursor( long relationshipId, ReadableTransactionState state )
    {
        return cursorPools.acquireSingleRelationshipCursor( relationshipId, state );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor( ReadableTransactionState state )
    {
        return cursorPools.relationshipsGetAllCursor( state );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( NodeItem nodeItem, Direction direction,
            ReadableTransactionState state )
    {
        return nodeGetRelationships( nodeItem, direction, null, state );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( NodeItem node, Direction direction, int[] relTypes,
            ReadableTransactionState state )
    {
        return cursorPools
                .acquireNodeRelationshipCursor( node.isDense(), node.id(), node.nextRelationshipId(), direction,
                relTypes, state );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( NodeItem node, PropertyContainerState state )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return cursorPools.acquirePropertyCursor( node.nextPropertyId(), lock, state );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperty( NodeItem node, int propertyKeyId, PropertyContainerState state )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return cursorPools.acquireSinglePropertyCursor( node.nextPropertyId(), propertyKeyId, lock, state );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( RelationshipItem relationship, PropertyContainerState state )
    {
        Lock lock =
                relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return cursorPools.acquirePropertyCursor( relationship.nextPropertyId(), lock, state );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperty( RelationshipItem relationship,
            int propertyKeyId, PropertyContainerState state )
    {
        Lock lock = relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return cursorPools.acquireSinglePropertyCursor( relationship.nextPropertyId(), propertyKeyId, lock, state );
    }

    @Override
    public long reserveNode()
    {
        return nodeStore.nextId();
    }

    @Override
    public long reserveRelationship()
    {
        return relationshipStore.nextId();
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
        if ( !(startLabelId == ReadOperations.ANY_LABEL || endLabelId == ReadOperations.ANY_LABEL) )
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
    public DoubleLongRegister indexUpdatesAndSize( LabelSchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return counts.indexUpdatesAndSize( tryGetIndexId( descriptor ), target );
    }

    @Override
    public DoubleLongRegister indexSample( LabelSchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return counts.indexSample( tryGetIndexId( descriptor ), target );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StoreReadLayer,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    private long tryGetIndexId( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexId( descriptor );
    }

    @Override
    public boolean nodeExists( long id )
    {
        return nodeStore.isInUse( id );
    }

    @Override
    public PrimitiveIntSet relationshipTypes( NodeItem node )
    {
        PrimitiveIntSet set = intSet();
        if ( node.isDense() )
        {
            cursorPools.acquireRelationshipGroupCursor( node.nextGroupId() ).forAll( group -> set.add( group.type() ) );
        }
        else
        {
            nodeGetRelationships( node, BOTH, EMPTY ).forAll( relationship -> set.add( relationship.type() ) );
        }
        return set;
    }

    @Override
    public void degrees( NodeItem node, DegreeVisitor visitor )
    {
        if ( node.isDense() )
        {
            try ( NodeDegreeCounter degreeCounter = cursorPools
                    .acquireNodeDegreeCounter( node.id(), node.nextGroupId() ) )
            {
                degreeCounter.accept( visitor );
            }
        }
        else
        {
            visitNode( node, visitor );
        }
    }

    @Override
    public int countDegrees( NodeItem node, Direction direction, ReadableTransactionState state )
    {
        int count;
        if ( state != null && state.nodeIsAddedInThisTx( node.id() ) )
        {
            count = 0;
        }
        else
        {
            if ( node.isDense() )
            {
                try ( NodeDegreeCounter degreeCounter = cursorPools
                        .acquireNodeDegreeCounter( node.id(), node.nextGroupId() ) )
                {
                    count = degreeCounter.count( direction );
                }
            }
            else
            {
                count = count( nodeGetRelationships( node, direction, EMPTY ) );
            }
        }

        return state == null ? count : state.getNodeState( node.id() ).augmentDegree( direction, count );
    }

    @Override
    public int countDegrees( NodeItem node, Direction direction, int relType, ReadableTransactionState state )
    {
        int count;
        if ( state != null && state.nodeIsAddedInThisTx( node.id() ) )
        {
            count = 0;
        }
        else
        {
            if ( node.isDense() )
            {
                try ( NodeDegreeCounter degreeCounter = cursorPools
                        .acquireNodeDegreeCounter( node.id(), node.nextGroupId() ) )
                {
                    count = degreeCounter.count( direction, relType );
                }
            }
            else
            {
                count = count( nodeGetRelationships( node, direction, new int[]{relType}, EMPTY ) );
            }
        }

        return state == null ? count : state.getNodeState( node.id() ).augmentDegree( direction, count, relType );
    }

    private void visitNode( NodeItem node, DegreeVisitor visitor )
    {
        try ( Cursor<RelationshipItem> relationships = nodeGetRelationships( node, BOTH, EMPTY ) )
        {
            while ( relationships.next() )
            {
                RelationshipItem rel = relationships.get();
                int type = rel.type();
                switch ( directionOf( node.id(), rel.id(), rel.startNode(), rel.endNode() ) )
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

    private Direction directionOf( long nodeId, long relationshipId, long startNode, long endNode )
    {
        if ( startNode == nodeId )
        {
            return endNode == nodeId ? BOTH : OUTGOING;
        }
        if ( endNode == nodeId )
        {
            return INCOMING;
        }
        throw new InvalidRecordException(
                "Node " + nodeId + " neither start nor end node of relationship " + relationshipId +
                        " with startNode:" + startNode + " and endNode:" + endNode );
    }

    private IndexRule indexRule( IndexDescriptor index )
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

    private static Iterator<IndexDescriptor> toIndexDescriptors( Iterable<IndexRule> rules )
    {
        return Iterators.map( IndexRule::getIndexDescriptor, rules.iterator() );
    }
}
