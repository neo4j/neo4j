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
package org.neo4j.kernel.impl.newapi;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;

public class MockStore extends Read implements TestRule
{
    private static final DynamicRecordAllocator NO_DYNAMIC_RECORDS = new DynamicRecordAllocator()
    {
        @Override
        public int getRecordDataSize()
        {
            throw new UnsupportedOperationException( "Should not allocate dynamic records" );
        }

        @Override
        public DynamicRecord nextRecord()
        {
            throw new UnsupportedOperationException( "Should not allocate dynamic records" );
        }
    };

    MockStore( DefaultCursors cursors )
    {
        super( cursors, mock( KernelTransactionImplementation.class ) );
    }

    @Override
    long graphPropertiesReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    IndexReader indexReader( IndexReference index, boolean fresh )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    LabelScanReader labelScanReader()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    ExplicitIndex explicitNodeIndex( String indexName )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    ExplicitIndex explicitRelationshipIndex( String indexName )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    PageCursor nodePage( long reference )
    {
        return null;
    }

    @Override
    PageCursor relationshipPage( long reference )
    {
        return null;
    }

    @Override
    PageCursor groupPage( long reference )
    {
        return null;
    }

    @Override
    PageCursor propertyPage( long reference )
    {
        return null;
    }

    @Override
    PageCursor stringPage( long reference )
    {
        return null;
    }

    @Override
    PageCursor arrayPage( long reference )
    {
        return null;
    }

    @Override
    RecordCursor<DynamicRecord> labelCursor()
    {
        return new RecordCursor<DynamicRecord>()
        {
            @Override
            public RecordCursor<DynamicRecord> acquire( long id, RecordLoad mode )
            {
                placeAt( id, mode );
                return this;
            }

            @Override
            public void placeAt( long id, RecordLoad mode )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public boolean next()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public boolean next( long id )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public boolean next( long id, DynamicRecord record, RecordLoad mode )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public void close()
            {
            }

            @Override
            public DynamicRecord get()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }
        };
    }

    @Override
    public boolean nodeExists( long id )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long countsForNode( int labelId )
    {
        throw new UnsupportedOperationException();

    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nodesGetCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long relationshipsGetCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipExists( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public UserFunctionHandle functionGet( QualifiedName name )
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public UserFunctionHandle aggregationFunctionGet( QualifiedName name )
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureHandle procedureGet( QualifiedName name ) throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );

    }

    @Override
    public Set<ProcedureSignature> proceduresGetAll(  ) throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );

    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallRead( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallReadOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWrite( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchema( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchemaOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallRead( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallReadOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWrite( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchema( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchemaOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public AnyValue functionCall( QualifiedName name, AnyValue[] arguments ) throws ProcedureException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public AnyValue functionCallOverride( QualifiedName name, AnyValue[] arguments ) throws ProcedureException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public UserAggregator aggregationFunction( QualifiedName name ) throws ProcedureException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public UserAggregator aggregationFunctionOverride( QualifiedName name ) throws ProcedureException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public ValueMapper<Object> valueMapper()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserAggregator aggregationFunction( int id ) throws ProcedureException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String,String> nodeExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] nodeExplicitIndexesGetAll()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String,String> relationshipExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        throw new UnsupportedOperationException();
    }

    private abstract static class Record<R extends AbstractBaseRecord>
    {
        abstract void initialize( R record );
    }

    private static class Node extends Record<NodeRecord>
    {
        private final long property;
        private final boolean dense;
        private final long edge;
        private final long labels;

        Node( long property, boolean dense, long edge, long labels )
        {
            this.property = property;
            this.dense = dense;
            this.edge = edge;
            this.labels = labels;
        }

        @Override
        void initialize( NodeRecord record )
        {
            record.initialize( true, property, dense, edge, labels );
        }
    }

    private static class Property extends Record<PropertyRecord>
    {
        private final long prev;
        private final long next;
        private final PropertyBlock[] payload;

        Property( long prev, long next, PropertyBlock[] payload )
        {
            this.prev = prev;
            this.next = next;
            this.payload = payload;
        }

        @Override
        void initialize( PropertyRecord record )
        {
            record.initialize( true, prev, next );
            for ( PropertyBlock block : payload )
            {
                for ( long value : block.getValueBlocks() )
                {
                    record.addLoadedBlock( value );
                }
            }
        }
    }

    private PrimitiveLongObjectMap<Node> nodes;
    private PrimitiveLongObjectMap<Property> properties;

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {

                try ( PrimitiveLongObjectMap<Node> nodes = Primitive.longObjectMap();
                      PrimitiveLongObjectMap<Property> properties = Primitive.longObjectMap() )
                {
                    MockStore.this.nodes = nodes;
                    MockStore.this.properties = properties;
                    base.evaluate();
                }
                finally
                {
                    MockStore.this.nodes = null;
                    MockStore.this.properties = null;
                }
            }
        };
    }

    private static <R extends AbstractBaseRecord, S extends Record<R>> void initialize(
            R record,
            long reference,
            PrimitiveLongObjectMap<S> store )
    {
        record.setId( reference );
        S node = store.get( reference );
        if ( node == null )
        {
            record.clear();
        }
        else
        {
            node.initialize( record );
        }
    }

    public void node( long id, long property, boolean dense, long edge, long labels )
    {
        nodes.put( id, new Node( property, dense, edge, labels ) );
    }

    public void property( long id, long prev, long next, PropertyBlock... payload )
    {
        properties.put( id, new Property( prev, next, payload ) );
    }

    public static PropertyBlock block( int key, Value value )
    {
        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, key, value, NO_DYNAMIC_RECORDS, NO_DYNAMIC_RECORDS, true );
        return block;
    }

    @Override
    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        initialize( record, reference, nodes );
    }

    @Override
    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        initialize( record, reference, properties );
    }

    @Override
    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    long nodeHighMark()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    long relationshipHighMark()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    TextValue string( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    ArrayValue array( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, Value value ) throws KernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Register.DoubleLongRegister indexSample( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        return null;
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K,V> creator )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public <K, V> V schemaStateGet( K key )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void schemaStateFlush()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
