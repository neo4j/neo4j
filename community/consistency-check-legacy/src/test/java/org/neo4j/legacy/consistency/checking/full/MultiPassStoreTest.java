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
package org.neo4j.legacy.consistency.checking.full;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;

import java.util.List;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.legacy.consistency.store.RecordReference.SkippingReference.skipReference;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        MultiPassStoreTest.Nodes.class,
        MultiPassStoreTest.Relationships.class,
        MultiPassStoreTest.Properties.class,
        MultiPassStoreTest.Strings.class,
        MultiPassStoreTest.Arrays.class
})
public abstract class MultiPassStoreTest
{
    @Test
    public void createsListOfFiltersWhichEachSkipRecordsOutsideOfARangeOfMappableIds() throws Exception
    {
        // given
        StoreAccess storeAccess = storeAccess( 1000L, 9 );
        DiffRecordAccess recordAccess = mock( DiffRecordAccess.class );

        long memoryPerPass = 900L;

        // when
        List<DiffRecordAccess> filters = multiPassStore().multiPassFilters(
                memoryPerPass, storeAccess, recordAccess, MultiPassStore.values() );

        // then
        assertEquals( 11, filters.size() );
        assertFinds( record( filters.get( 0 ), 0 ) );
        assertFinds( record( filters.get( 0 ), 99 ) );
        assertFinds( record( filters.get( 1 ), 100 ) );
        assertFinds( record( filters.get( 1 ), 199 ) );
        assertFinds( record( filters.get( 2 ), 200 ) );
        assertFinds( record( filters.get( 2 ), 299 ) );
        assertFinds( record( filters.get( 10 ), 1000 ) );

        assertSkips( record( filters.get( 1 ), 0 ) );
        assertSkips( record( filters.get( 1 ), 99 ) );
        assertSkips( record( filters.get( 2 ), 100 ) );
        assertSkips( record( filters.get( 2 ), 199 ) );
        assertSkips( record( filters.get( 0 ), 100 ) );
        assertSkips( record( filters.get( 0 ), 199 ) );
        assertSkips( record( filters.get( 1 ), 200 ) );
        assertSkips( record( filters.get( 1 ), 299 ) );
    }

    @Test
    public void shouldSkipOtherKindsOfRecords() throws Exception
    {
        // given
        StoreAccess storeAccess = storeAccess( 1000L, 9 );
        DiffRecordAccess recordAccess = mock( DiffRecordAccess.class );

        long memoryPerPass = 900L;

        // when
        List<DiffRecordAccess> filters = multiPassStore().multiPassFilters(
                memoryPerPass, storeAccess, recordAccess, MultiPassStore.values() );

        // then
        for ( DiffRecordAccess filter : filters )
        {
            for ( long id : new long[] {0, 100, 200, 300, 400, 500, 600, 700, 800, 900} )
            {
                otherRecords( filter, id );
            }
        }

        verifyZeroInteractions( recordAccess );
    }

    private static <RECORD extends AbstractBaseRecord> void assertSkips( RecordReference<RECORD> recordReference )
    {
        assertSame( skipReference(), recordReference );
    }

    private static <RECORD extends AbstractBaseRecord> void assertFinds( RecordReference<RECORD> recordReference )
    {
        assertNotSame( skipReference(), recordReference );
    }

    @SuppressWarnings("unchecked")
    private StoreAccess storeAccess( long highId, int recordSize )
    {
        StoreAccess storeAccess = mock( StoreAccess.class );
        RecordStore recordStore = mock( RecordStore.class );
        when( multiPassStore().getRecordStore( storeAccess ) ).thenReturn( recordStore );
        when( recordStore.getHighId() ).thenReturn( highId );
        when( recordStore.getRecordSize() ).thenReturn( recordSize );
        return storeAccess;
    }

    protected abstract MultiPassStore multiPassStore();

    protected abstract RecordReference<? extends AbstractBaseRecord> record( DiffRecordAccess filter, long id );

    protected abstract void otherRecords( DiffRecordAccess filter, long id );

    @RunWith(JUnit4.class)
    public static class Nodes extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.NODES;
        }

        @Override
        protected RecordReference<NodeRecord> record( DiffRecordAccess filter, long id )
        {
            return filter.node( id );
        }

        protected void otherRecords( DiffRecordAccess filter, long id )
        {
            filter.relationship( id );
            filter.property( id );
            filter.string( id );
            filter.array( id );
        }
    }

    @RunWith(JUnit4.class)
    public static class Relationships extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.RELATIONSHIPS;
        }

        @Override
        protected RecordReference<RelationshipRecord> record( DiffRecordAccess filter, long id )
        {
            return filter.relationship( id );
        }

        protected void otherRecords( DiffRecordAccess filter, long id )
        {
            filter.node( id );
            filter.property( id );
            filter.string( id );
            filter.array( id );
        }
    }

    @RunWith(JUnit4.class)
    public static class Properties extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.PROPERTIES;
        }

        @Override
        protected RecordReference<PropertyRecord> record( DiffRecordAccess filter, long id )
        {
            return filter.property( id );
        }

        protected void otherRecords( DiffRecordAccess filter, long id )
        {
            filter.node( id );
            filter.relationship( id );
            filter.string( id );
            filter.array( id );
        }
    }

    @RunWith(JUnit4.class)
    public static class Strings extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.STRINGS;
        }

        @Override
        protected RecordReference<DynamicRecord> record( DiffRecordAccess filter, long id )
        {
            return filter.string( id );
        }

        protected void otherRecords( DiffRecordAccess filter, long id )
        {
            filter.node( id );
            filter.relationship( id );
            filter.property( id );
            filter.array( id );
        }
    }

    @RunWith(JUnit4.class)
    public static class Arrays extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.ARRAYS;
        }

        @Override
        protected RecordReference<DynamicRecord> record( DiffRecordAccess filter, long id )
        {
            return filter.array( id );
        }

        protected void otherRecords( DiffRecordAccess filter, long id )
        {
            filter.node( id );
            filter.relationship( id );
            filter.property( id );
            filter.string( id );
        }
    }
}
