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
package org.neo4j.consistency.checking.full;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;

import java.util.List;

import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

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
    public void shouldSkipOtherKindsOfRecords() throws Exception
    {
        // given
        RecordAccess recordAccess = mock( RecordAccess.class );

        // when
        List<RecordAccess> filters = multiPassStore().multiPassFilters( recordAccess, MultiPassStore.values() );

        // then
        for ( RecordAccess filter : filters )
        {
            for ( long id : new long[] {0, 100, 200, 300, 400, 500, 600, 700, 800, 900} )
            {
                otherRecords( filter, id );
            }
        }

        verifyZeroInteractions( recordAccess );
    }

    protected abstract MultiPassStore multiPassStore();

    protected abstract RecordReference<? extends AbstractBaseRecord> record( RecordAccess filter, long id );

    protected abstract void otherRecords( RecordAccess filter, long id );

    @RunWith(JUnit4.class)
    public static class Nodes extends MultiPassStoreTest
    {
        @Override
        protected MultiPassStore multiPassStore()
        {
            return MultiPassStore.NODES;
        }

        @Override
        protected RecordReference<NodeRecord> record( RecordAccess filter, long id )
        {
            return filter.node( id );
        }

        @Override
        protected void otherRecords( RecordAccess filter, long id )
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
        protected RecordReference<RelationshipRecord> record( RecordAccess filter, long id )
        {
            return filter.relationship( id );
        }

        @Override
        protected void otherRecords( RecordAccess filter, long id )
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
        protected RecordReference<PropertyRecord> record( RecordAccess filter, long id )
        {
            return filter.property( id );
        }

        @Override
        protected void otherRecords( RecordAccess filter, long id )
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
        protected RecordReference<DynamicRecord> record( RecordAccess filter, long id )
        {
            return filter.string( id );
        }

        @Override
        protected void otherRecords( RecordAccess filter, long id )
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
        protected RecordReference<DynamicRecord> record( RecordAccess filter, long id )
        {
            return filter.array( id );
        }

        @Override
        protected void otherRecords( RecordAccess filter, long id )
        {
            filter.node( id );
            filter.relationship( id );
            filter.property( id );
            filter.string( id );
        }
    }
}
