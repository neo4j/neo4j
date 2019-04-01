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

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.stream;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.PropertySchemaType.COMPLETE_ALL_TOKENS;

@ExtendWith( RandomExtension.class )
class SchemaDescriptorLookupSetTest
{
    @Inject
    private RandomRule random;

    @ParameterizedTest
    @EnumSource( DescriptorFactory.class )
    void shouldLookupSingleKeyDescriptors( DescriptorFactory factory )
    {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        SchemaDescriptor expected = factory.descriptor( 1, 2 );
        set.add( expected );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForPartialListOfProperties( descriptors, longs( 1 ), ints( 2 ) );

        // then
        assertEquals( asSet( expected ), descriptors );
    }

    @ParameterizedTest
    @EnumSource( DescriptorFactory.class )
    void shouldLookupSingleKeyAndSharedCompositeKeyDescriptors( DescriptorFactory factory )
    {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        SchemaDescriptor expected1 = factory.descriptor( 1, 2 );
        SchemaDescriptor expected2 = factory.descriptor( 1, 2, 3 );
        set.add( expected1 );
        set.add( expected2 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForPartialListOfProperties( descriptors, longs( 1 ), ints( 2 ) );

        // then
        assertEquals( asSet( expected1, expected2 ), descriptors );
    }

    @ParameterizedTest
    @EnumSource( DescriptorFactory.class )
    void shouldLookupCompositeKeyDescriptor( DescriptorFactory factory )
    {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        SchemaDescriptor descriptor1 = factory.descriptor( 1, 2, 3 );
        SchemaDescriptor descriptor2 = factory.descriptor( 1, 2, 4 );
        SchemaDescriptor descriptor3 = factory.descriptor( 1, 2, 5, 6 );
        set.add( descriptor1 );
        set.add( descriptor2 );
        set.add( descriptor3 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptorsForCompleteListOfProperties( descriptors, longs( 1 ), ints( 2, 5, 6 ) );

        // then
        assertEquals( asSet( descriptor3 ), descriptors );
    }

    @ParameterizedTest
    @EnumSource( DescriptorFactory.class )
    void shouldLookupAllByEntityToken( DescriptorFactory factory )
    {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        SchemaDescriptor descriptor1 = factory.descriptor( 1, 2, 3 );
        SchemaDescriptor descriptor2 = factory.descriptor( 1, 2, 4 );
        SchemaDescriptor descriptor3 = factory.descriptor( 1, 2, 5, 6 );
        SchemaDescriptor descriptor4 = factory.descriptor( 2, 2, 3 );
        SchemaDescriptor descriptor5 = factory.descriptor( 3, 2, 5, 6 );
        set.add( descriptor1 );
        set.add( descriptor2 );
        set.add( descriptor3 );
        set.add( descriptor4 );
        set.add( descriptor5 );

        // when
        Set<SchemaDescriptor> descriptors = new HashSet<>();
        set.matchingDescriptors( descriptors, longs( 1 ) );

        // then
        assertEquals( asSet( descriptor1, descriptor2, descriptor3 ), descriptors );
    }

    @ParameterizedTest
    @MethodSource( "nodeAndRelationshipEntityTypes" )
    void shouldMatchOnAnyEntityAndPropertyTokenForPartialPropertySchemaType( EntityType entityType )
    {
        // given
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        MultiTokenSchemaDescriptor descriptor1 = SchemaDescriptorFactory.multiToken( ints( 0, 1, 2 ), entityType, 3, 4, 5 );
        MultiTokenSchemaDescriptor descriptor2 = SchemaDescriptorFactory.multiToken( ints( 0, 1 ), entityType, 3, 4 );
        MultiTokenSchemaDescriptor descriptor3 = SchemaDescriptorFactory.multiToken( ints( 0, 2 ), entityType, 4, 5 );
        set.add( descriptor1 );
        set.add( descriptor2 );
        set.add( descriptor3 );

        // given that this test revolves around entity tokens 0,1,2 and property tokens 3,4,5 these 3 descriptors below matches either
        // only those tokens for entity or property or neither. I.e. these should never be included in matching results
        set.add( SchemaDescriptorFactory.multiToken( ints( 3, 4 ), entityType, 4, 5 ) );
        set.add( SchemaDescriptorFactory.multiToken( ints( 0, 1 ), entityType, 6, 7 ) );
        set.add( SchemaDescriptorFactory.multiToken( ints( 3, 4 ), entityType, 6, 7 ) );

        // when matching these descriptors (in this case partial/complete list doesn't quite matter because the descriptors
        // themselves are partially matched anyway.
        Set<SchemaDescriptor> descriptors1 = new HashSet<>();
        Set<SchemaDescriptor> descriptors1Partial = new HashSet<>();
        Set<SchemaDescriptor> descriptors2 = new HashSet<>();
        Set<SchemaDescriptor> descriptors2Partial = new HashSet<>();
        Set<SchemaDescriptor> descriptors3 = new HashSet<>();
        Set<SchemaDescriptor> descriptors3Partial = new HashSet<>();
        set.matchingDescriptorsForCompleteListOfProperties( descriptors1, longs( 0, 1 ), ints( 4, 5 ) );
        set.matchingDescriptorsForPartialListOfProperties( descriptors1Partial, longs( 0, 1 ), ints( 4, 5 ) );
        set.matchingDescriptorsForCompleteListOfProperties( descriptors2, longs( 0 ), ints( 3 ) );
        set.matchingDescriptorsForPartialListOfProperties( descriptors2Partial, longs( 0 ), ints( 3 ) );
        set.matchingDescriptorsForCompleteListOfProperties( descriptors3, longs( 1 ), ints( 5 ) );
        set.matchingDescriptorsForPartialListOfProperties( descriptors3Partial, longs( 1 ), ints( 5 ) );

        // then
        assertEquals( asSet( descriptor1, descriptor2, descriptor3 ), descriptors1 );
        assertEquals( asSet( descriptor1, descriptor2, descriptor3 ), descriptors1Partial );
        assertEquals( asSet( descriptor1, descriptor2 ), descriptors2 );
        assertEquals( asSet( descriptor1, descriptor2 ), descriptors2Partial );
        assertEquals( asSet( descriptor1 ), descriptors3 );
        assertEquals( asSet( descriptor1 ), descriptors3Partial );
    }

    @Test
    void shouldAddRemoveAndLookupRandomDescriptorsNoIdempotentOperations()
    {
        shouldAddRemoveAndLookupRandomDescriptors( false );
    }

    @Test
    void shouldAddRemoveAndLookupRandomDescriptorsWithIdempotentOperations()
    {
        shouldAddRemoveAndLookupRandomDescriptors( true );
    }

    private void shouldAddRemoveAndLookupRandomDescriptors( boolean includeIdempotentAddsAndRemoves )
    {
        // given
        List<SchemaDescriptor> all = new ArrayList<>();
        SchemaDescriptorLookupSet<SchemaDescriptor> set = new SchemaDescriptorLookupSet<>();
        int highEntityKeyId = 8;
        int highPropertyKeyId = 8;
        int maxNumberOfEntityKeys = 3;
        int maxNumberOfPropertyKeys = 3;

        // when/then
        for ( int i = 0; i < 100 /*just a couple of rounds adding, removing and looking up descriptors*/; i++ )
        {
            // add some
            int countToAdd = random.nextInt( 1, 5 );
            for ( int a = 0; a < countToAdd; a++ )
            {
                SchemaDescriptor descriptor = randomSchemaDescriptor( highEntityKeyId, highPropertyKeyId, maxNumberOfEntityKeys, maxNumberOfPropertyKeys );
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
                int[] entityTokenIdsInts = randomUniqueSortedIntArray( highEntityKeyId, random.nextInt( 1, 3 ) );
                long[] entityTokenIds = toLongArray( entityTokenIdsInts );
                int[] propertyKeyIds = randomUniqueSortedIntArray( highPropertyKeyId, random.nextInt( 1, maxNumberOfPropertyKeys ) );
                Set<SchemaDescriptor> actual = new HashSet<>();

                // lookup by only entity tokens
                actual.clear();
                set.matchingDescriptors( actual, entityTokenIds );
                assertEquals( expectedDescriptors( all, filterByEntity( entityTokenIdsInts ) ), actual );

                // lookup by partial property list
                actual.clear();
                set.matchingDescriptorsForPartialListOfProperties( actual, entityTokenIds, propertyKeyIds );
                assertEquals( expectedDescriptors( all, filterByEntityAndPropertyPartial( entityTokenIdsInts, propertyKeyIds ) ), actual );

                // lookup by complete property list
                actual.clear();
                set.matchingDescriptorsForCompleteListOfProperties( actual, entityTokenIds, propertyKeyIds );
                assertEquals( expectedDescriptors( all, filterByEntityAndPropertyComplete( entityTokenIdsInts, propertyKeyIds ) ), actual );
            }
        }
    }

    private static Predicate<SchemaDescriptor> filterByEntityAndPropertyComplete( int[] entityTokenIds, int[] propertyKeyIds )
    {
        return descriptor ->
        {
            IntPredicate propertyKeyPredicate = indexPropertyId -> contains( propertyKeyIds, indexPropertyId );
            boolean propertiesAccepted = descriptor.propertySchemaType() == COMPLETE_ALL_TOKENS
                    // For typical indexes (COMPLETE_ALL_TOKENS) all must match
                    ? stream( descriptor.getPropertyIds() ).allMatch( propertyKeyPredicate )
                    // For multi-token (e.g. full-text) descriptors any property key match is to be considered a match
                    : stream( descriptor.getPropertyIds() ).anyMatch( propertyKeyPredicate );
            return stream( descriptor.getEntityTokenIds() ).anyMatch( indexEntityId -> contains( entityTokenIds, indexEntityId ) ) && propertiesAccepted;
        };
    }

    private static Predicate<SchemaDescriptor> filterByEntityAndPropertyPartial( int[] entityTokenIds, int[] propertyKeyIds )
    {
        return descriptor ->
                stream( descriptor.getEntityTokenIds() ).anyMatch( indexEntityId -> contains( entityTokenIds, indexEntityId ) ) &&
                stream( descriptor.getPropertyIds() ).anyMatch( indexPropertyId -> contains( propertyKeyIds, indexPropertyId ) );
    }

    private static Predicate<SchemaDescriptor> filterByEntity( int[] entityTokenIds )
    {
        return descriptor -> stream( descriptor.getEntityTokenIds() ).anyMatch( indexEntityId -> contains( entityTokenIds, indexEntityId ) );
    }

    private static Set<SchemaDescriptor> expectedDescriptors( List<SchemaDescriptor> all, Predicate<SchemaDescriptor> filter )
    {
        return asSet( Iterators.filter( filter, all.iterator() ) );
    }

    private SchemaDescriptor randomSchemaDescriptor( int highEntityKeyId, int highPropertyKeyId, int maxNumberOfEntityKeys, int maxNumberOfPropertyKeys )
    {
        int numberOfEntityKeys = random.nextInt( 1, maxNumberOfEntityKeys );
        int[] entityKeys = randomUniqueUnsortedIntArray( highEntityKeyId, numberOfEntityKeys );
        int numberOfPropertyKeys = random.nextInt( 1, maxNumberOfPropertyKeys );
        int[] propertyKeys = randomUniqueUnsortedIntArray( highPropertyKeyId, numberOfPropertyKeys );
        return entityKeys.length > 1
               ? SchemaDescriptorFactory.multiToken( entityKeys, EntityType.NODE, propertyKeys )
               : SchemaDescriptorFactory.forLabel( entityKeys[0], propertyKeys );
    }

    private int[] randomUniqueUnsortedIntArray( int maxValue, int length )
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

    private int[] randomUniqueSortedIntArray( int maxValue, int length )
    {
        int[] array = randomUniqueUnsortedIntArray( maxValue, length );
        Arrays.sort( array );
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

    private static int[] ints( int... properties )
    {
        return properties;
    }

    private static long[] longs( long... labels )
    {
        return labels;
    }

    private static EntityType[] nodeAndRelationshipEntityTypes()
    {
        return new EntityType[]{EntityType.NODE, EntityType.RELATIONSHIP};
    }

    enum DescriptorFactory
    {
        NODE
                {
                    @Override
                    SchemaDescriptor descriptor( int labelId, int... propertyKeyIds )
                    {
                        return SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
                    }
                },
        RELATIONSHIP
                {
                    @Override
                    SchemaDescriptor descriptor( int relTypeId, int... propertyKeyIds )
                    {
                        return SchemaDescriptorFactory.forRelType( relTypeId, propertyKeyIds );
                    }
                },
        ;

        abstract SchemaDescriptor descriptor( int entityTokenId, int... propertyKeyIds );
    }
}
