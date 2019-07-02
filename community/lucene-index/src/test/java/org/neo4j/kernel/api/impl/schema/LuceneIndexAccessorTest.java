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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LuceneIndexAccessorTest
{
    private final SchemaIndex schemaIndex = mock( SchemaIndex.class );

    private LuceneIndexAccessor accessor;

    @BeforeEach
    void setUp()
    {
        accessor = new LuceneIndexAccessor( schemaIndex, TestIndexDescriptorFactory.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ) );
    }

    @Test
    void indexIsDirtyWhenLuceneIndexIsNotValid()
    {
        when( schemaIndex.isValid() ).thenReturn( false );
        assertTrue( accessor.isDirty() );
    }

    @Test
    void indexIsCleanWhenLuceneIndexIsValid()
    {
        when( schemaIndex.isValid() ).thenReturn( true );
        assertFalse( accessor.isDirty() );
    }
}
