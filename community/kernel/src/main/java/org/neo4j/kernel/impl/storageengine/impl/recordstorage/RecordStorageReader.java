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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

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
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
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
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TransactionalDependencies;
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

import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.toDescriptor;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.INCOMING;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.LOOP;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.OUTGOING;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.GroupReferenceEncoding.isRelationship;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 *
 * TODO Public due to a usage in BatchInserter, which hasn't been ported yet to use the RecordStorageEngine as a whole
 * It's a small price to pay for not having to rewrite a lot of that code right now.
 */
public class RecordStorageReader extends DefaultCursors implements StorageReader, TxStateHolder
{
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
    private final RecordStorageCommandCreationContext commandCreationContext;

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;
    private TransactionalDependencies transactionalDependencies = TransactionalDependencies.EMPTY;
    private DefaultNodeValueIndexCursor nodeValueIndexCursorForUniquenessCheck;

    public RecordStorageReader( PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
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
    }

    /**
     * This method shows that this reader has too many responsibilities. This factory is here to create a reader of
     * basic record stores through the cursors. Mostly convenience in some special scenarios.
     *
     * @param neoStores {@link NeoStores} to provide reading for.
     * @return a {@link RecordStorageReader} only capable of reading basic records through the cursors.
     */
    public static RecordStorageReader neoStoreReader( NeoStores neoStores )
    {
        return new RecordStorageReader( null, null, null, null, neoStores, null, null, null, null, null );
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
    public Iterator<SchemaIndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
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
        return countsForNode( labelId );
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
        return countsForRelationship( startLabelId, typeId, endLabelId );
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
    public DoubleLongRegister indexUpdatesAndSize( IndexReference index, DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return counts.indexUpdatesAndSize( tryGetIndexId( toDescriptor( index ).schema() ), target );
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

    private IndexRule indexRule( IndexReference index )
    {
        SchemaIndexDescriptor descriptor = toDescriptor( index );
        for ( IndexRule rule : schemaCache.indexRules() )
        {
            if ( rule.getIndexDescriptor().equals( descriptor ) )
            {
                return rule;
            }
        }

        return schemaStorage.indexGetForSchema( descriptor );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    @Override
    public void initialize( TransactionalDependencies transactionalDependencies )
    {
        this.transactionalDependencies = transactionalDependencies;
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
        releaseCursors();
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

    LabelScanReader getLabelScanReader()
    {
        return labelScanReader != null ?
               labelScanReader : (labelScanReader = labelScanReaderSupplier.get());
    }

    private IndexReaderFactory indexReaderFactory()
    {
        return indexReaderFactory != null ?
               indexReaderFactory : (indexReaderFactory = indexReaderFactorySupplier.get());
    }

    private IndexReader getIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newReader( descriptor );
    }

    private IndexReader getFreshIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
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
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        IndexReader reader = indexReader( index, true );
        DefaultNodeValueIndexCursor cursor = nodeValueIndexCursorForUniquenessCheck();
        cursor.setRead( this, reader );
        IndexProgressor.NodeValueClient target = withFullValuePrecision( cursor, predicates, reader );
        reader.query( target, IndexOrder.NONE, predicates );
        return cursor.next() ? cursor.nodeReference() : NO_ID;
    }

    private DefaultNodeValueIndexCursor nodeValueIndexCursorForUniquenessCheck()
    {
        if ( nodeValueIndexCursorForUniquenessCheck == null )
        {
            nodeValueIndexCursorForUniquenessCheck = new DefaultNodeValueIndexCursor( null );
        }
        return nodeValueIndexCursorForUniquenessCheck;
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
            if ( !transactionalDependencies.securityContext().mode().allowsPropertyReads( prop ) )
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
        return transactionalDependencies.securityContext().mode();
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
        transactionalDependencies.assertOpen();
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        SchemaIndexDescriptor descriptor = schemaCache.indexDescriptor( SchemaDescriptorFactory.forLabel( label, properties ) );
        if ( descriptor == null )
        {
            return CapableIndexReference.NO_INDEX;
        }

        boolean unique = descriptor.type() == SchemaIndexDescriptor.Type.UNIQUE;
        SchemaDescriptor schema = descriptor.schema();
        IndexProxy indexProxy;
        try
        {
            indexProxy = indexService.getIndexProxy( schema );
        }
        catch ( IndexNotFoundKernelException e )
        {
            return CapableIndexReference.NO_INDEX;
        }

        return new DefaultCapableIndexReference( unique, indexProxy.getIndexCapability(),
                indexProxy.getProviderDescriptor(), schema.keyId(),
                schema.getPropertyIds() );
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
        return getIndexProxy( DefaultIndexReference.toDescriptor( index ) ).getState();
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
        IndexRule rule = indexRule( index );
        if ( rule == null )
        {
            SchemaIndexDescriptor descriptor = toDescriptor( index );
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
        return transactionalDependencies.txState();
    }

    @Override
    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return transactionalDependencies.explicitIndexTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return transactionalDependencies.hasTxStateWithChanges();
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

    @Override
    public NodeCursor allocateNodeCursorCommitted()
    {
        return new DefaultNodeCursor( null, false );
    }

    @Override
    public PropertyCursor allocatePropertyCursorCommitted()
    {
        return new DefaultPropertyCursor( null, false );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursorCommitted()
    {
        return new DefaultRelationshipScanCursor( null, false );
    }

    @Override
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursorCommitted()
    {
        return new DefaultRelationshipGroupCursor( null, false );
    }
}
