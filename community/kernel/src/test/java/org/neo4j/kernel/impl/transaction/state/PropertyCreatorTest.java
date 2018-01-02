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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.impl.store.AbstractRecordStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.unsafe.batchinsert.DirectRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PropertyCreatorTest
{
    private final IdSequence idGenerator = new BatchingIdSequence();
    private final PropertyCreator creator = new PropertyCreator( null, null, idGenerator, new PropertyTraverser() );

    // The RecordAccess will take on the role of both store and tx state and the PropertyCreator
    // will know no difference
    @SuppressWarnings( "unchecked" )
    private final RecordAccess<Long,PropertyRecord,PrimitiveRecord> records =
            new DirectRecordAccess<>( mock( AbstractRecordStore.class ), new PropertyRecordLoader() );
    private final MyPrimitiveProxy primitive = new MyPrimitiveProxy();

    @Test
    public void shouldAddPropertyToEmptyChain() throws Exception
    {
        // GIVEN
        existingChain();

        // WHEN
        setProperty( 1, "value" );

        // THEN
        assertChain( record( property( 1, "value" ) ) );
    }

    @Test
    public void shouldAddPropertyToChainContainingOtherFullRecords() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 7, 7 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 10, 10 ) ),
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 7, 7 ) ) );
    }

    @Test
    public void shouldAddPropertyToChainContainingOtherNonFullRecords() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 10, 10 ) ) );
    }

    @Test
    public void shouldAddPropertyToChainContainingOtherNonFullRecordsInMiddle() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 10, 10 ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );
    }

    @Test
    public void shouldChangeOnlyProperty() throws Exception
    {
        // GIVEN
        existingChain( record( property( 0, "one" ) ) );

        // WHEN
        setProperty( 0, "two" );

        // THEN
        assertChain( record( property( 0, "two" ) ) );
    }

    @Test
    public void shouldChangePropertyInChainWithOthersBeforeIt() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 2, "two*" );

        // THEN
        assertChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two*" ), property( 3, 3 ) ) );
    }

    @Test
    public void shouldChangePropertyInChainWithOthersAfterIt() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 0, "one*" );

        // THEN
        assertChain(
                record( property( 0, "one*" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );
    }

    @Test
    public void shouldChangePropertyToBiggerInFullChain() throws Exception
    {
        // GIVEN
        existingChain( record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 1, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 1, Long.MAX_VALUE ) ),
                record( property( 0, 0 ), property( 2, 2 ), property( 3, 3 ) ) );
    }

    @Test
    public void shouldChangePropertyToBiggerInChainWithHoleAfter() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ) ) );

        // WHEN
        setProperty( 1, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 1, Long.MAX_VALUE ) ) );
    }

    // change property so that it gets bigger and fits in a record earlier in the chain
    @Test
    public void shouldChangePropertyToBiggerInChainWithHoleBefore() throws Exception
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ) ),
                record( property( 2, 2 ), property( 3, 3 ), property( 4, 4 ), property( 5, 5 ) ) );

        // WHEN
        setProperty( 2, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, Long.MAX_VALUE ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ) ) );
    }

    private void existingChain( ExpectedRecord... initialRecords )
    {
        PropertyRecord prev = null;
        for ( ExpectedRecord initialRecord : initialRecords )
        {
            PropertyRecord record = this.records.create( idGenerator.nextId(), primitive.record ).forChangingData();
            record.setInUse( true );
            existingRecord( record, initialRecord );

            if ( prev == null )
            {
                // This is the first one, update primitive to point to this
                primitive.record.setNextProp( record.getId() );
            }
            else
            {
                // link property records together
                record.setPrevProp( prev.getId() );
                prev.setNextProp( record.getId() );
            }


            prev = record;
        }
    }

    private void existingRecord( PropertyRecord record, ExpectedRecord initialRecord )
    {
        for ( ExpectedProperty initialProperty : initialRecord.properties )
        {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue( block, initialProperty.key, initialProperty.value, null, null );
            record.addPropertyBlock( block );
        }
        assertTrue( record.size() <= PropertyType.getPayloadSize() );
    }

    private void setProperty( int key, Object value )
    {
        creator.primitiveSetProperty( primitive, key, value, records );
    }

    private void assertChain( ExpectedRecord... expectedRecords )
    {
        long nextProp = primitive.forReadingLinkage().getNextProp();
        int expectedRecordCursor = 0;
        while ( !Record.NO_NEXT_PROPERTY.is( nextProp ) )
        {
            PropertyRecord record = records.getIfLoaded( nextProp ).forReadingData();
            assertRecord( record, expectedRecords[expectedRecordCursor++] );
            nextProp = record.getNextProp();
        }
    }

    private void assertRecord( PropertyRecord record, ExpectedRecord expectedRecord )
    {
        assertEquals( expectedRecord.properties.length, record.numberOfProperties() );
        for ( ExpectedProperty expectedProperty : expectedRecord.properties )
        {
            PropertyBlock block = record.getPropertyBlock( expectedProperty.key );
            assertNotNull( block );
            assertEquals( expectedProperty.value, block.getType().getValue( block, null ) );
        }
    }

    private static class ExpectedProperty
    {
        private final int key;
        private final Object value;

        ExpectedProperty( int key, Object value )
        {
            super();
            this.key = key;
            this.value = value;
        }
    }

    private static class ExpectedRecord
    {
        private final ExpectedProperty[] properties;

        ExpectedRecord( ExpectedProperty... properties )
        {
            this.properties = properties;
        }
    }

    static ExpectedProperty property( int key, Object value )
    {
        return new ExpectedProperty( key, value );
    }

    private ExpectedRecord record( ExpectedProperty... properties )
    {
        return new ExpectedRecord( properties );
    }

    private static class MyPrimitiveProxy implements RecordProxy<Long,NodeRecord,Void>
    {
        private final NodeRecord record = new NodeRecord( 5 );
        private boolean changed;

        MyPrimitiveProxy()
        {
            record.setInUse( true );
        }

        @Override
        public Long getKey()
        {
            return record.getId();
        }

        @Override
        public NodeRecord forChangingLinkage()
        {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forChangingData()
        {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forReadingLinkage()
        {
            return record;
        }

        @Override
        public NodeRecord forReadingData()
        {
            return record;
        }

        @Override
        public Void getAdditionalData()
        {
            return null;
        }

        @Override
        public NodeRecord getBefore()
        {
            return record;
        }

        @Override
        public boolean isChanged()
        {
            return changed;
        }

        @Override
        public boolean isCreated()
        {
            return false;
        }
    }

    private static class PropertyRecordLoader implements Loader<Long,PropertyRecord,PrimitiveRecord>
    {
        private final Loader<Long,PropertyRecord,PrimitiveRecord> actual = Loaders.propertyLoader( null );

        @Override
        public PropertyRecord newUnused( Long key, PrimitiveRecord additionalData )
        {
            return actual.newUnused( key, additionalData );
        }

        @Override
        public PropertyRecord load( Long key, PrimitiveRecord additionalData )
        {
            return null;
        }

        @Override
        public void ensureHeavy( PropertyRecord record )
        {
        }

        @Override
        public PropertyRecord clone( PropertyRecord record )
        {
            return record.clone();
        }
    }
}
