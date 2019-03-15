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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class LabelPropertyMultiSetTest
{
    @Test
    public void shouldLookupSingleKeyDescriptors()
    {
        // given
        LabelPropertyMultiSet set = new LabelPropertyMultiSet();
        LabelSchemaDescriptor expected = SchemaDescriptorFactory.forLabel( 1, 2 );
        set.add( expected );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForPartialListOfProperties( descriptors, labels( 1 ), properties( 2 ) );

        // then
        assertEquals( asSet( expected ), descriptors );
    }

    @Test
    public void shouldLookupSingleKeyAndSharedCompositeKeyDescriptors()
    {
        // given
        LabelPropertyMultiSet set = new LabelPropertyMultiSet();
        LabelSchemaDescriptor expected1 = SchemaDescriptorFactory.forLabel( 1, 2 );
        LabelSchemaDescriptor expected2 = SchemaDescriptorFactory.forLabel( 1, 2, 3 );
        set.add( expected1 );
        set.add( expected2 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForPartialListOfProperties( descriptors, labels( 1 ), properties( 2 ) );

        // then
        assertEquals( asSet( expected1, expected2 ), descriptors );
    }

    @Test
    public void shouldLookupCompositeKeyDescriptor()
    {
        // given
        LabelPropertyMultiSet set = new LabelPropertyMultiSet();
        LabelSchemaDescriptor descriptor1 = SchemaDescriptorFactory.forLabel( 1, 2, 3 );
        LabelSchemaDescriptor descriptor2 = SchemaDescriptorFactory.forLabel( 1, 2, 4 );
        LabelSchemaDescriptor descriptor3 = SchemaDescriptorFactory.forLabel( 1, 2, 5, 6 );
        set.add( descriptor1 );
        set.add( descriptor2 );
        set.add( descriptor3 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForCompleteListOfProperties( descriptors, labels( 1 ), properties( 2, 5, 6 ) );

        // then
        assertEquals( asSet( descriptor3 ), descriptors );
    }

    @Test
    public void shouldLookupAllByLabel()
    {
        // given
        LabelPropertyMultiSet set = new LabelPropertyMultiSet();
        LabelSchemaDescriptor descriptor1 = SchemaDescriptorFactory.forLabel( 1, 2, 3 );
        LabelSchemaDescriptor descriptor2 = SchemaDescriptorFactory.forLabel( 1, 2, 4 );
        LabelSchemaDescriptor descriptor3 = SchemaDescriptorFactory.forLabel( 1, 2, 5, 6 );
        LabelSchemaDescriptor descriptor4 = SchemaDescriptorFactory.forLabel( 2, 2, 3 );
        LabelSchemaDescriptor descriptor5 = SchemaDescriptorFactory.forLabel( 3, 2, 5, 6 );
        set.add( descriptor1 );
        set.add( descriptor2 );
        set.add( descriptor3 );
        set.add( descriptor4 );
        set.add( descriptor5 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptors( descriptors, labels( 1 ) );

        // then
        assertEquals( asSet( descriptor1, descriptor2, descriptor3 ), descriptors );
    }

    private static int[] properties( int... properties )
    {
        return properties;
    }

    private static long[] labels( long... labels )
    {
        return labels;
    }
}
