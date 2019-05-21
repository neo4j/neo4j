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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;

import static java.util.Optional.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

class FulltextIndexDescriptorTest
{
    @Test
    void updatingIndexProviderLeavesOriginalDescriptorUntouched()
    {
        FulltextIndexDescriptor a = new FulltextIndexDescriptor( new DefaultStorageIndexReference(
                forLabel( 1, 2 ), "provider-A", "1.0", 1, empty(), false, null, false ), new String[] { "prop" }, new StandardAnalyzer(), "standard", false );
        FulltextIndexDescriptor b  = a.withIndexProvider( new IndexProviderDescriptor( "provider-B", "2.0" ) );

        assertEquals( b.providerKey(), "provider-B" );
        assertEquals( b.providerVersion(), "2.0" );
        assertEquals( a.providerKey(), "provider-A" );
        assertEquals( a.providerVersion(), "1.0" );
    }

    @Test
    void updatingSchemaDescriptorLeavesOriginalDescriptorUntouched()
    {
        FulltextIndexDescriptor a = new FulltextIndexDescriptor( new DefaultStorageIndexReference(
                forLabel( 1, 2 ), "provider-A", "1.0", 1, empty(), false, null, false ), new String[] { "prop" }, new StandardAnalyzer(), "standard", false );
        FulltextIndexDescriptor b  = a.withSchemaDescriptor( forLabel( 10, 20 ) );

        assertEquals( b.schema(), forLabel( 10, 20 ) );
        assertEquals( a.schema(), forLabel( 1, 2 ) );
    }

    @Test
    void updatingEventualConsistencyFlagLeavesOriginalDescriptorUntouched()
    {
        FulltextIndexDescriptor a = new FulltextIndexDescriptor( new DefaultStorageIndexReference(
                forLabel( 1, 2 ), "provider-A", "1.0", 1, empty(), false, null, false ), new String[] { "prop" }, new StandardAnalyzer(), "standard", false );
        FulltextIndexDescriptor b  = a.withEventualConsistency( true );

        assertTrue( b.isEventuallyConsistent() );
        assertFalse( a.isEventuallyConsistent() );
    }
}
