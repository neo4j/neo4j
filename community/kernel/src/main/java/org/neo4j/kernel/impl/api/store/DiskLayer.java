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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;
import org.neo4j.register.Register;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.register.Registers.newDoubleLongRegister;

/**
 * Default implementation of StoreReadLayer. Delegates to NeoStores and indexes.
 */
public class DiskLayer implements StoreReadLayer
{
    private static final Function<PropertyConstraintRule, PropertyConstraint> RULE_TO_CONSTRAINT =
            new Function<PropertyConstraintRule, PropertyConstraint>()
            {
                @Override
                public PropertyConstraint apply( PropertyConstraintRule rule )
                {
                    // We can use propertyKeyId straight up here, without reading from the record, since we have
                    // verified that it has that propertyKeyId in the predicate. And since we currently only support
                    // uniqueness on single properties, there is nothing else to pass in to UniquenessConstraint.
                    return rule.toConstraint();
                }
            };

    private static final Function<NodePropertyConstraintRule,NodePropertyConstraint> NODE_RULE_TO_CONSTRAINT =
            new Function<NodePropertyConstraintRule,NodePropertyConstraint>()
            {
                @Override
                public NodePropertyConstraint apply( NodePropertyConstraintRule rule )
                {
                    return rule.toConstraint();
                }
            };

    private static final Function<RelationshipPropertyConstraintRule,RelationshipPropertyConstraint> REL_RULE_TO_CONSTRAINT =
            new Function<RelationshipPropertyConstraintRule,RelationshipPropertyConstraint>()
            {
                @Override
                public RelationshipPropertyConstraint apply( RelationshipPropertyConstraintRule rule )
                {
                    return rule.toConstraint();
                }
            };

    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTokenHolder;

    private final NeoStores neoStores;
    private final IndexingService indexService;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final SchemaStorage schemaStorage;
    private final CountsAccessor counts;
    private final PropertyLoader propertyLoader;

    private final Factory<StoreStatement> statementProvider;

    public DiskLayer( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage, NeoStores neoStores,
            IndexingService indexService, Factory<StoreStatement> statementProvider )
    {
        this.relationshipTokenHolder = relationshipTokenHolder;
        this.schemaStorage = schemaStorage;
        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.neoStores = neoStores;
        this.statementProvider = statementProvider;
        this.nodeStore = this.neoStores.getNodeStore();
        this.relationshipStore = this.neoStores.getRelationshipStore();
        this.counts = neoStores.getCounts();
        this.propertyLoader = new PropertyLoader( neoStores );
    }

    @Override
    public StoreStatement acquireStatement()
    {
        return statementProvider.newInstance();
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
            if ( e.getCause() instanceof UnderlyingStorageException
                    && e.getCause().getMessage().equals( "Id capacity exceeded" ) )
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
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        return state.getLabelScanReader().nodesWithLabel( labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException
    {
        IndexReader reader = state.getIndexReader( index );
        return reader.seek( value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromInclusiveNumericIndexRangeSeek( KernelStatement statement,
            IndexDescriptor index,
            Number lower,
            Number upper )
            throws IndexNotFoundKernelException

    {
        IndexReader reader = statement.getIndexReader( index );
        return reader.rangeSeekByNumberInclusive( lower, upper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement statement,
                                                                     IndexDescriptor index,
                                                                     String lower, boolean includeLower,
                                                                     String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        IndexReader reader = statement.getIndexReader( index );
        return reader.rangeSeekByString( lower, includeLower, upper, includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state,
                                                                     IndexDescriptor index,
                                                                     String prefix )
            throws IndexNotFoundKernelException
    {
        IndexReader reader = state.getIndexReader( index );
        return reader.rangeSeekByPrefix( prefix );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index ) throws
            IndexNotFoundKernelException
    {
        IndexReader reader = state.getIndexReader( index );
        return reader.scan();
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey )
    {
        return descriptor( schemaStorage.indexRule( labelId, propertyKey ) );
    }

    private static IndexDescriptor descriptor( IndexRule ruleRecord )
    {
        return new IndexDescriptor( ruleRecord.getLabel(), ruleRecord.getPropertyKey() );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return getIndexDescriptorsFor( indexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return getIndexDescriptorsFor( INDEX_RULES );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        return getIndexDescriptorsFor( constraintIndexRules( labelId ) );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        return getIndexDescriptorsFor( CONSTRAINT_INDEX_RULES );
    }

    private static Predicate<SchemaRule> indexRules( final int labelId )
    {
        return new Predicate<SchemaRule>()
        {

            @Override
            public boolean test( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == SchemaRule.Kind.INDEX_RULE;
            }
        };
    }

    private static Predicate<SchemaRule> constraintIndexRules( final int labelId )
    {
        return new Predicate<SchemaRule>()
        {

            @Override
            public boolean test( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
            }
        };
    }

    private static final Predicate<SchemaRule> INDEX_RULES = new Predicate<SchemaRule>()
    {

        @Override
        public boolean test( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.INDEX_RULE;
        }
    }, CONSTRAINT_INDEX_RULES = new Predicate<SchemaRule>()
    {

        @Override
        public boolean test( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
        }
    };

    private Iterator<IndexDescriptor> getIndexDescriptorsFor( Predicate<SchemaRule> filter )
    {
        Iterator<SchemaRule> filtered = filter( filter, neoStores.getSchemaStore().loadAllSchemaRules() );

        return map( new Function<SchemaRule, IndexDescriptor>()
        {

            @Override
            public IndexDescriptor apply( SchemaRule from )
            {
                return descriptor( (IndexRule) from );
            }
        }, filtered );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getOwningConstraint();
    }

    @Override
    public IndexRule indexRule( IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
            throws SchemaRuleNotFoundException
    {
        return schemaStorage.indexRule( index.getLabelId(), index.getPropertyKeyId() ).getId();
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getState();
    }

    @Override
    public long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        Register.DoubleLongRegister result = indexService.indexUpdatesAndSize(descriptor);
        return result.readSecond();
    }

    @Override
    public double indexUniqueValuesPercentage( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.indexUniqueValuesPercentage( descriptor );
    }

    @Override
    public Iterator<ProcedureDescriptor> proceduresGetAll()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureDescriptor procedureGet( ProcedureSignature.ProcedureName name )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getPopulationFailure().asString();
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, final int propertyKeyId )
    {
        return schemaStorage.schemaRulesForNodes( NODE_RULE_TO_CONSTRAINT, NodePropertyConstraintRule.class,
                labelId, new Predicate<NodePropertyConstraintRule>()
                {
                    @Override
                    public boolean test( NodePropertyConstraintRule rule )
                    {
                        return rule.containsPropertyKeyId( propertyKeyId );
                    }
                } );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId )
    {
        return schemaStorage.schemaRulesForNodes( NODE_RULE_TO_CONSTRAINT, NodePropertyConstraintRule.class,
                labelId, Predicates.<NodePropertyConstraintRule>alwaysTrue() );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            final int propertyKeyId )
    {
        return schemaStorage.schemaRulesForRelationships( REL_RULE_TO_CONSTRAINT,
                RelationshipPropertyConstraintRule.class, typeId, new Predicate<RelationshipPropertyConstraintRule>()
                {
                    @Override
                    public boolean test( RelationshipPropertyConstraintRule rule )
                    {
                        return rule.containsPropertyKeyId( propertyKeyId );
                    }
                } );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId )
    {
        return schemaStorage.schemaRulesForRelationships( REL_RULE_TO_CONSTRAINT,
                RelationshipPropertyConstraintRule.class, typeId,
                Predicates.<RelationshipPropertyConstraintRule>alwaysTrue() );
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll()
    {
        return schemaStorage.schemaRules( RULE_TO_CONSTRAINT, PropertyConstraintRule.class );
    }

    @Override
    public PrimitiveLongResourceIterator nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor descriptor,
            Object value ) throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        /* Here we have an intricate scenario where we need to return the PrimitiveLongIterator
         * since subsequent filtering will happen outside, but at the same time have the ability to
         * close the IndexReader when done iterating over the lookup result. This is because we get
         * a fresh reader that isn't associated with the current transaction and hence will not be
         * automatically closed. */
        IndexReader reader = state.getFreshIndexReader( descriptor );
        return resourceIterator( reader.seek( value ), reader );
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
    public String propertyKeyGetName( int propertyKeyId )
            throws PropertyKeyIdNotFoundKernelException
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
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        return new PropertyKeyIdIterator( propertyLoader.graphLoadProperties( new IteratingPropertyReceiver() ) );
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties()
    {
        return propertyLoader.graphLoadProperties( new IteratingPropertyReceiver() );
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
        RelationshipRecord record;
        try
        {
            record = relationshipStore.getRecord( relationshipId );
        }
        catch ( InvalidRecordException e )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        relationshipVisitor.visit( relationshipId, record.getType(), record.getFirstNode(), record.getSecondNode() );
    }

    @Override
    public long highestNodeIdInUse()
    {
        return nodeStore.getHighestPossibleIdInUse();
    }

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        return new PrimitiveLongBaseIterator()
        {
            private long highId = nodeStore.getHighestPossibleIdInUse();
            private long currentId;
            private final NodeRecord reusableNodeRecord = new NodeRecord( -1 ); // reused

            @Override
            protected boolean fetchNext()
            {
                while ( true )
                {   // This outer loop is for checking if highId has changed since we started.
                    while ( currentId <= highId )
                    {
                        try
                        {
                            NodeRecord record = nodeStore.loadRecord( currentId, reusableNodeRecord );
                            if ( record != null && record.inUse() )
                            {
                                return next( record.getId() );
                            }
                        }
                        finally
                        {
                            currentId++;
                        }
                    }

                    long newHighId = nodeStore.getHighestPossibleIdInUse();
                    if ( newHighId > highId )
                    {
                        highId = newHighId;
                    }
                    else
                    {
                        break;
                    }
                }
                return false;
            }
        };
    }

    @Override
    public RelationshipIterator relationshipsGetAll()
    {
        return new RelationshipIterator.BaseIterator()
        {
            private long highId = relationshipStore.getHighestPossibleIdInUse();
            private long currentId;
            private final RelationshipRecord reusableRecord = new RelationshipRecord( -1 ); // reused

            @Override
            protected boolean fetchNext()
            {
                while ( true )
                {   // This outer loop is for checking if highId has changed since we started.
                    while ( currentId <= highId )
                    {
                        try
                        {
                            if ( relationshipStore.fillRecord( currentId, reusableRecord, CHECK ) && reusableRecord.inUse() )
                            {
                                return next( reusableRecord.getId() );
                            }
                        }
                        finally
                        {
                            currentId++;
                        }
                    }

                    long newHighId = relationshipStore.getHighestPossibleIdInUse();
                    if ( newHighId > highId )
                    {
                        highId = newHighId;
                    }
                    else
                    {
                        break;
                    }
                }
                return false;
            }

            @Override
            public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                    RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
            {
                visitor.visit( relationshipId, reusableRecord.getType(),
                        reusableRecord.getFirstNode(), reusableRecord.getSecondNode() );
                return false;
            }
        };
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
}
