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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.kernel.api.schema.index.IndexDescriptor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class LuceneIndexAccessorTest
{
    @Mock
    private SchemaIndex schemaIndex;
    @Mock
    private IndexDescriptor indexDescriptor;
    private LuceneIndexAccessor accessor;

    @Before
    public void setUp() throws IOException
    {
        accessor = new LuceneIndexAccessor( schemaIndex, indexDescriptor );
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
}
