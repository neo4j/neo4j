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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationHandler;

import org.neo4j.kernel.impl.annotations.ReporterFactories;
import org.neo4j.kernel.impl.annotations.ReporterFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class LuceneIndexAccessorTest
{
    @Mock
    private SchemaIndex schemaIndex;
    @Mock
    private IndexDescriptor schemaIndexDescriptor;
    private LuceneIndexAccessor accessor;

    @Before
    public void setUp()
    {
        accessor = new LuceneIndexAccessor( schemaIndex, schemaIndexDescriptor );
    }

    @Test
    public void indexIsDirtyWhenLuceneIndexIsNotValid()
    {
        when( schemaIndex.isValid() ).thenReturn( false );
        assertTrue( accessor.isDirty() );
    }

    @Test
    public void indexIsCleanWhenLuceneIndexIsValid()
    {
        when( schemaIndex.isValid() ).thenReturn( true );
        assertFalse( accessor.isDirty() );
    }

    @Test
    public void indexIsNotConsistentWhenIndexIsNotValid()
    {
        when( schemaIndex.isValid() ).thenReturn( false );
        assertFalse( accessor.consistencyCheck( ReporterFactories.noopReporterFactory() ) );
    }

    @Test
    public void indexIsConsistentWhenIndexIsValid()
    {
        when( schemaIndex.isValid() ).thenReturn( true );
        assertTrue( accessor.consistencyCheck( ReporterFactories.noopReporterFactory() ) );
    }

    @Test
    public void indexReportInconsistencyToVisitor()
    {
        when( schemaIndex.isValid() ).thenReturn( false );
        MutableBoolean called = new MutableBoolean();
        final InvocationHandler handler = ( proxy, method, args ) -> {
            called.setTrue();
            return null;
        };
        assertFalse( "Expected index to be inconsistent", accessor.consistencyCheck( new ReporterFactory( handler ) ) );
        assertTrue( "Expected visitor to be called", called.booleanValue() );
    }
}
