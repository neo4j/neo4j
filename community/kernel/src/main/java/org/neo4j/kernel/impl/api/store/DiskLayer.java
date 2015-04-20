/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
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
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class DiskLayer implements StoreReadLayer
{
    private static final Function<UniquenessConstraintRule, UniquenessConstraint> UNIQUENESS_CONSTRAINT_TO_RULE =
            new Function<UniquenessConstraintRule, UniquenessConstraint>()
    {

        @Override
        public UniquenessConstraint apply( UniquenessConstraintRule rule )
        {
            // We can use propertyKeyId straight up here, without reading from the record, since we have
            // verified that it has that propertyKeyId in the predicate. And since we currently only support
            // uniqueness on single properties, there is nothing else to pass in to UniquenessConstraint.
            return new UniquenessConstraint( rule.getLabel(), rule.getPropertyKey() );
        }
    };

    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTokenHolder;

    private final NeoStore neoStore;
    private final IndexingService indexService;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final SchemaStorage schemaStorage;
    private final Provider<PropertyStore> propertyStoreProvider;
    private final CountsAccessor counts;

    private static class PropertyStoreProvider implements Provider<PropertyStore>
    {
        private final Provider<NeoStore> neoStoreProvider;

        public PropertyStoreProvider( Provider<NeoStore> neoStoreProvider )
        {
            this.neoStoreProvider = neoStoreProvider;
        }

        @Override
        public PropertyStore instance()
        {
            return neoStoreProvider.instance().getPropertyStore();
        }
    }

    /**
     * A note on this taking Provider<NeoStore> rather than just neo store: This is a workaround until the cache is
     * removed. Because the neostore may be restarted while the database is running, and because lazy properties keep
     * a reference to the property store, we need a way to resolve the property store on demand for properties in the
     * cache. As such, this takes a provider, and uses that provider to provide property store references when resolving
     * lazy properties.
     */
    public DiskLayer( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                      RelationshipTypeTokenHolder relationshipTokenHolder, SchemaStorage schemaStorage,
                      final Provider<NeoStore> neoStoreProvider, IndexingService indexService )
    {
        this.relationshipTokenHolder = relationshipTokenHolder;
        this.schemaStorage = schemaStorage;
        this.indexService = indexService;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.neoStore = neoStoreProvider.instance();
        this.nodeStore = this.neoStore.getNodeStore();
        this.relationshipStore = this.neoStore.getRelationshipStore();
        this.propertyStore = this.neoStore.getPropertyStore();
        this.propertyStoreProvider = new PropertyStoreProvider( neoStoreProvider );
        this.counts = neoStore.getCounts();
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
            else
            {
                throw e;
            }
        }
    }

    @Override
    public int labelGetForName( String label )
    {
        return labelTokenHolder.getIdByName( label );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId )
    {
        try
        {
            return PrimitiveIntCollections.indexOf( nodeGetLabels( nodeId ), labelId ) != -1;
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId )
    {
        NodeRecord record = nodeStore.loadRecord( nodeId, null );
        if ( record == null )
        {
            return PrimitiveIntCollections.emptyIterator();
        }

        final long[] labels = parseLabelsField( record ).get( nodeStore );
        return new PrimitiveIntIterator()
        {
            private int cursor;


            @Override
            public boolean hasNext()
            {
                return cursor < labels.length;
            }


            @Override
            public int next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return safeCastLongToInt( labels[cursor++] );
            }
        };
    }

    @Override
    public PrimitiveLongIterator nodeListRelationships( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveLongIterator nodeListRelationships( long nodeId, Direction direction, int[] relTypes ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException( "expected to be answered by cache" );
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException( "expected to be answered by cache" );
    }

    @Override
    public boolean nodeVisitDegrees( long nodeId, DegreeVisitor visitor )
    {
        throw new UnsupportedOperationException( "expected to be answered by cache" );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
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
    public PrimitiveLongResourceIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value
    ) throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException();
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
            public boolean accept( SchemaRule rule )
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
            public boolean accept( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
            }
        };
    }

    private static final Predicate<SchemaRule> INDEX_RULES = new Predicate<SchemaRule>()
    {

        @Override
        public boolean accept( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.INDEX_RULE;
        }
    }, CONSTRAINT_INDEX_RULES = new Predicate<SchemaRule>()
    {

        @Override
        public boolean accept( SchemaRule rule )
        {
            return rule.getKind() == SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
        }
    };

    private Iterator<IndexDescriptor> getIndexDescriptorsFor( Predicate<SchemaRule> filter )
    {
        Iterator<SchemaRule> filtered = filter( filter, neoStore.getSchemaStore().loadAllSchemaRules() );

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
    public boolean nodeExists( long nodeId )
    {
        return nodeStore.inUse( nodeId );
    }

    @Override
    public boolean relationshipExists( long relationshipId )
    {
        return relationshipStore.inUse( relationshipId );
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
        return indexService.getIndexProxy( indexId( descriptor ) ).getState();
    }

    @Override
    public double indexUniqueValuesPercentage( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.indexUniqueValuesPercentage( indexId( descriptor ) );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( indexId( descriptor ) ).getPopulationFailure().asString();
    }

    private long indexId( IndexDescriptor descriptor )
    {
        return schemaStorage.indexRule( descriptor.getLabelId(), descriptor.getPropertyKeyId() ).getId();
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( int labelId, final int propertyKeyId )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, UniquenessConstraintRule.class,
                labelId, new Predicate<UniquenessConstraintRule>()
        {
            @Override
            public boolean accept( UniquenessConstraintRule rule )
            {
                return rule.containsPropertyKeyId( propertyKeyId );
            }
        } );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( int labelId )
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, UniquenessConstraintRule.class,
                labelId, Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        return schemaStorage.schemaRules( UNIQUENESS_CONSTRAINT_TO_RULE, SchemaRule.Kind.UNIQUENESS_CONSTRAINT,
                Predicates.<UniquenessConstraintRule>TRUE() );
    }

    @Override
    public PrimitiveLongResourceIterator nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index,
                                                                       Object value ) throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        throw new UnsupportedOperationException();
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
    public PrimitiveLongIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Property nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( long nodeId )
            throws EntityNotFoundException
    {
        try
        {
            return loadAllPropertiesOf( nodeStore.getRecord( nodeId ) );
        }
        catch ( InvalidRecordException e )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId, e );
        }
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( long relationshipId )
            throws EntityNotFoundException
    {
        try
        {
            return loadAllPropertiesOf( relationshipStore.getRecord( relationshipId ) );
        }
        catch ( InvalidRecordException e )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId, e );
        }
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Property graphGetProperty( int propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties()
    {
        return loadAllPropertiesOf( neoStore.asRecord() );
    }

    public PrimitiveLongResourceIterator nodeGetUniqueFromIndexLookup( KernelStatement state,
            long indexId, Object value )
            throws IndexNotFoundKernelException
    {
        /* Here we have an intricate scenario where we need to return the PrimitiveLongIterator
         * since subsequent filtering will happen outside, but at the same time have the ability to
         * close the IndexReader when done iterating over the lookup result. This is because we get
         * a fresh reader that isn't associated with the current transaction and hence will not be
         * automatically closed. */
        IndexReader reader = state.getFreshIndexReader( indexId );
        return resourceIterator( reader.lookup( value ), reader );
    }

    public PrimitiveLongResourceIterator nodesGetFromIndexLookup( KernelStatement state, long index, Object value )
            throws IndexNotFoundKernelException
    {
        IndexReader reader = state.getIndexReader( index );
        return resourceIterator( reader.lookup( value ), reader );
    }

    private Iterator<DefinedProperty> loadAllPropertiesOf( PrimitiveRecord primitiveRecord )
    {
        Collection<PropertyRecord> records = propertyStore.getPropertyRecordChain( primitiveRecord.getNextProp() );
        if ( null == records )
        {
            return IteratorUtil.emptyIterator();
        }
        List<DefinedProperty> properties = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock block : record )
            {
                properties.add( block.getType().readProperty( block.getKeyIndexId(), block, propertyStoreProvider ) );
            }
        }
        return properties.iterator();
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
            return ((Token)relationshipTokenHolder.getTokenById( relationshipTypeId )).name();
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
    public <EXCEPTION extends Exception> void relationshipVisit( long relationshipId, RelationshipVisitor<EXCEPTION> relationshipVisitor ) throws EntityNotFoundException, EXCEPTION
    {
        throw new UnsupportedOperationException();
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
            private final NodeStore store = neoStore.getNodeStore();
            private long highId = store.getHighestPossibleIdInUse();
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
                            NodeRecord record = store.loadRecord( currentId, reusableNodeRecord );
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

                    long newHighId = store.getHighestPossibleIdInUse();
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
    public PrimitiveLongIterator relationshipsGetAll()
    {
        return new PrimitiveLongBaseIterator()
        {
            private final RelationshipStore store = neoStore.getRelationshipStore();
            private long highId = store.getHighestPossibleIdInUse();
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
                            if ( store.fillRecord( currentId, reusableRecord, CHECK ) && reusableRecord.inUse() )
                            {
                                return next( reusableRecord.getId() );
                            }
                        }
                        finally
                        {
                            currentId++;
                        }
                    }

                    long newHighId = store.getHighestPossibleIdInUse();
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
    public Cursor expand( Cursor inputCursor, NeoRegister.Node.In nodeId, Register.Object.In<int[]> types, Register
            .Object.In<Direction> expandDirection, NeoRegister.Relationship.Out relId, NeoRegister.RelType.Out
            relType, Register.Object.Out<Direction> direction, NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out
            neighborNodeId )
    {
        throw new UnsupportedOperationException();
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
}
