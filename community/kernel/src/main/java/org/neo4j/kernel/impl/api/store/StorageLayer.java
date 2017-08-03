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
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
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
import org.neo4j.kernel.impl.api.RelationshipVisitor;
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
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.collection.primitive.Primitive.intSet;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.kernel.impl.api.store.DegreeCounter.countByFirstPrevPointer;
import static org.neo4j.kernel.impl.api.store.DegreeCounter.countRelationshipsInGroup;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.register.Registers.newDoubleLongRegister;

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
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final SchemaStorage schemaStorage;
    private final CountsTracker counts;
    private final PropertyLoader propertyLoader;
    private final Supplier<StorageStatement> statementProvider;
    private final SchemaCache schemaCache;

    public StorageLayer( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage, NeoStores neoStores,
            IndexingService indexService, Supplier<StorageStatement> storeStatementSupplier, SchemaCache schemaCache )
    {
        this.relationshipTokenHolder = relationshipTokenHolder;
        this.schemaStorage = schemaStorage;
        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.statementProvider = storeStatementSupplier;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.counts = neoStores.getCounts();
        this.propertyLoader = new PropertyLoader( neoStores );
        this.schemaCache = schemaCache;
    }

    @Override
    public StorageStatement newStatement()
    {
        return statementProvider.get();
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
    public PrimitiveLongIterator nodesGetForLabel( StorageStatement statement, int labelId )
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
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
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
    public Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem nodeItem,
            Direction direction )
    {
        return nodeGetRelationships( statement, nodeItem, direction, ALWAYS_TRUE_INT );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem node,
            Direction direction, IntPredicate relTypes )
    {
        return statement.acquireNodeRelationshipCursor( node.isDense(), node.id(), node.nextRelationshipId(), direction,
                relTypes );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( StorageStatement statement, NodeItem node, AssertOpen assertOpen )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return statement.acquirePropertyCursor( node.nextPropertyId(), lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperty( StorageStatement statement, NodeItem node, int propertyKeyId,
            AssertOpen assertOpen )
    {
        Lock lock = node.lock(); // lock before reading the property id, since we might need to reload the record
        return statement.acquireSinglePropertyCursor( node.nextPropertyId(), propertyKeyId, lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( StorageStatement statement, RelationshipItem relationship,
            AssertOpen assertOpen )
    {
        Lock lock = relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return statement.acquirePropertyCursor( relationship.nextPropertyId(), lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperty( StorageStatement statement, RelationshipItem relationship,
            int propertyKeyId, AssertOpen assertOpen )
    {
        Lock lock = relationship.lock(); // lock before reading the property id, since we might need to reload the record
        return statement.acquireSinglePropertyCursor( relationship.nextPropertyId(), propertyKeyId, lock, assertOpen );
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
    public PrimitiveIntSet relationshipTypes( StorageStatement statement, NodeItem node )
    {
        PrimitiveIntSet set = intSet();
        if ( node.isDense() )
        {
            RelationshipGroupRecord groupRecord = relationshipGroupStore.newRecord();
            RecordCursor<RelationshipGroupRecord> cursor = statement.recordCursors().relationshipGroup();
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
            nodeGetRelationships( statement, node, Direction.BOTH )
                    .forAll( relationship -> set.add( relationship.type() ) );
        }
        return set;
    }

    @Override
    public void degrees( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor )
    {
        if ( nodeItem.isDense() )
        {
            visitDenseNode( statement, nodeItem, visitor );
        }
        else
        {
            visitNode( statement, nodeItem, visitor );
        }
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

    @Override
    public int degreeRelationshipsInGroup( StorageStatement storeStatement, long nodeId, long groupId,
            Direction direction, Integer relType )
    {
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        RelationshipGroupRecord relationshipGroupRecord = relationshipGroupStore.newRecord();
        return countRelationshipsInGroup( groupId, direction, relType, nodeId, relationshipRecord,
                relationshipGroupRecord, storeStatement.recordCursors() );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StoreReadLayer,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    private void visitNode( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor )
    {
        try ( Cursor<RelationshipItem> relationships = nodeGetRelationships( statement, nodeItem, Direction.BOTH ) )
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

    private void visitDenseNode( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor )
    {
        RelationshipGroupRecord relationshipGroupRecord = relationshipGroupStore.newRecord();
        RecordCursor<RelationshipGroupRecord> relationshipGroupCursor = statement.recordCursors().relationshipGroup();
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        RecordCursor<RelationshipRecord> relationshipCursor = statement.recordCursors().relationship();

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

    private static Iterator<IndexDescriptor> toIndexDescriptors( Iterable<IndexRule> rules )
    {
        return Iterators.map( IndexRule::getIndexDescriptor, rules.iterator() );
    }
}
