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
package org.neo4j.internal.schema;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.rule.RandomRule;

import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class LabelPropertyMultiSetTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldLookupSingleKeyDescriptors()
    {
        // given
        LabelPropertyMultiSet<SchemaDescriptor> set = new LabelPropertyMultiSet<>();
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
        LabelPropertyMultiSet<SchemaDescriptor> set = new LabelPropertyMultiSet<>();
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
        LabelPropertyMultiSet<SchemaDescriptor> set = new LabelPropertyMultiSet<>();
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
        LabelPropertyMultiSet<SchemaDescriptor> set = new LabelPropertyMultiSet<>();
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

    @Test
    public void shouldAddRemoveAndLookupRandomDescriptorsNoIdempotentOperations()
    {
        shouldAddRemoveAndLookupRandomDescriptors( false );
    }

    @Test
    public void shouldAddRemoveAndLookupRandomDescriptorsWithIdempotentOperations()
    {
        shouldAddRemoveAndLookupRandomDescriptors( true );
    }

    private void shouldAddRemoveAndLookupRandomDescriptors( boolean includeIdempotentAddsAndRemoves )
    {
        // given
        List<SchemaDescriptor> all = new ArrayList<>();
        LabelPropertyMultiSet<SchemaDescriptor> set = new LabelPropertyMultiSet<>();
        int highLabelId = 10;
        int highPropertyKeyId = 10;
        int maxNumberOfPropertyKeys = 3;

        // when/then
        for ( int i = 0; i < 100 /*just a couple of rounds adding, removing and looking up descriptors*/; i++ )
        {
            // add some
            int countToAdd = random.nextInt( 1, 5 );
            for ( int a = 0; a < countToAdd; a++ )
            {
                SchemaDescriptor descriptor = randomSchemaDescriptor( highLabelId, highPropertyKeyId, maxNumberOfPropertyKeys );
                if ( !includeIdempotentAddsAndRemoves && all.indexOf( descriptor ) != -1 )
                {
                    // Oops, we randomly generated a descriptor that already exists
                    continue;
                }

                set.add( descriptor );
                all.add( descriptor );
            }

            // remove some
            int countToRemove = random.nextInt( 0, 2 );
            for ( int r = 0; r < countToRemove && !all.isEmpty(); r++ )
            {
                SchemaDescriptor descriptor = all.remove( random.nextInt( all.size() ) );
                set.remove( descriptor );
                if ( includeIdempotentAddsAndRemoves )
                {
                    set.remove( descriptor );
                    while ( all.remove( descriptor ) )
                    {
                        // Just continue removing duplicates until all are done
                    }
                }
            }

            // lookup
            int countToLookup = 20;
            for ( int l = 0; l < countToLookup; l++ )
            {
                long[] labelIds = toLongArray( randomUniqueIntArray( highLabelId, random.nextInt( 1, 3 ) ) );
                int[] propertyKeyIds = randomUniqueIntArray( highPropertyKeyId, random.nextInt( 1, maxNumberOfPropertyKeys ) );
                Arrays.sort( propertyKeyIds );
                Set<SchemaDescriptor> actual = new HashSet<>();

                // lookup by only labels
                actual.clear();
                set.matchingDescriptors( actual, labelIds );
                assertEquals( expectedDescriptors( all, filterByLabel( labelIds ) ), actual );

                // lookup by partial property list
                actual.clear();
                set.matchingDescriptorsForPartialListOfProperties( actual, labelIds, propertyKeyIds );
                assertEquals( expectedDescriptors( all, filterByLabelAndPropertyPartial( labelIds, propertyKeyIds ) ), actual );

                // lookup by complete property list
                actual.clear();
                set.matchingDescriptorsForCompleteListOfProperties( actual, labelIds, propertyKeyIds );
                assertEquals( expectedDescriptors( all, filterByLabelAndPropertyComplete( labelIds, propertyKeyIds ) ), actual );
            }
        }
    }

    private static Predicate<SchemaDescriptor> filterByLabelAndPropertyComplete( long[] labelIds, int[] propertyKeyIds )
    {
        return descriptor ->
        {
            // Descriptor label matches any of my labels
            if ( !contains( labelIds, descriptor.keyId() ) )
            {
                return false;
            }
            // Descriptor property keys matches ALL of my property keys
            for ( int propertyKeyId : descriptor.getPropertyIds() )
            {
                if ( !contains( propertyKeyIds, propertyKeyId ) )
                {
                    return false;
                }
            }
            return true;
        };
    }

    private static Predicate<SchemaDescriptor> filterByLabelAndPropertyPartial( long[] labelIds, int[] propertyKeyIds )
    {
        return descriptor ->
        {
            // Descriptor label matches any of my labels
            if ( !contains( labelIds, descriptor.keyId() ) )
            {
                return false;
            }
            // Descriptor property keys matches any of my property keys
            for ( int propertyKeyId : propertyKeyIds )
            {
                if ( contains( descriptor.getPropertyIds(), propertyKeyId ) )
                {
                    return true;
                }
            }
            return false;
        };
    }

    private static Predicate<SchemaDescriptor> filterByLabel( long[] labelIds )
    {
        return descriptor -> contains( labelIds, descriptor.keyId() );
    }

    private static Set<SchemaDescriptor> expectedDescriptors( List<SchemaDescriptor> all, Predicate<SchemaDescriptor> filter )
    {
        return asSet( Iterators.filter( filter, all.iterator() ) );
    }

    private SchemaDescriptor randomSchemaDescriptor( int highLabelId, int highPropertyKeyId, int maxNumberOfPropertyKeys )
    {
        int labelId = random.nextInt( highLabelId );
        int numberOfPropertyKeys = random.nextInt( 1, maxNumberOfPropertyKeys );
        int[] propertyKeyIds = randomUniqueIntArray( highPropertyKeyId, numberOfPropertyKeys );
        return SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
    }

    private int[] randomUniqueIntArray( int maxValue, int length )
    {
        int[] array = new int[length];
        MutableIntSet seen = IntSets.mutable.empty();
        for ( int i = 0; i < length; i++ )
        {
            int candidate;
            do
            {
                candidate = random.nextInt( maxValue );
            }
            while ( !seen.add( candidate ) );
            array[i] = candidate;
        }
        return array;
    }

    private static long[] toLongArray( int[] array )
    {
        long[] result = new long[array.length];
        for ( int i = 0; i < array.length; i++ )
        {
            result[i] = array[i];
        }
        return result;
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
