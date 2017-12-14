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
package org.neo4j.kernel.impl.newapi;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.txstate.TxStateHolder;
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
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
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

    MockStore( Cursors cursors )
    {
        super( cursors, mock( TxStateHolder.class ), AssertOpen.ALWAYS_OPEN );
    }

    @Override
    long graphPropertiesReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    IndexReader indexReader( org.neo4j.internal.kernel.api.IndexReference index )
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
    TextValue string( PropertyCursor cursor, long reference, PageCursor page )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    ArrayValue array( PropertyCursor cursor, long reference, PageCursor page )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
