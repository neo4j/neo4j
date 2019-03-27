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

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;

import static java.lang.Math.toIntExact;

/**
 * Collects and provides efficient access to {@link SchemaDescriptor}, based on complete list of entity tokens and partial or complete list of property keys.
 * The descriptors are first grouped by entity token and then further grouped by property key. The grouping works on sorted token lists
 * to minimize deduplication when doing lookups, especially for composite and multi-entity descriptors.
 * <p>
 * The selection works best when providing a complete list of property keys and will have to resort to some amount of over-selection
 * when caller can only provide partial list of property keys. Selection starts by traversing through the entity tokens, and for each
 * found leaf continues traversing through the property keys. When adding descriptors the token lists are sorted and token lists passed
 * into lookup methods also have to be sorted. This way the internal data structure can be represented as chains of tokens and traversal only
 * needs to go through the token chains in one order (the sorted order). A visualization of the internal data structure of added descriptors:
 *
 * <pre>
 *     Legend: ids inside [] are entity tokens, ids inside () are properties
 *     Descriptors
 *     A: [0](4, 7, 3)
 *     B: [0](7, 4)
 *     C: [0](3, 4)
 *     D: [0](3, 4, 7)
 *     E: [1](7)
 *     F: [1](5, 6)
 *     G: [0, 1](3, 4)
 *     H: [1, 0](3)
 *
 *     Will result in a data structure (for the optimized path):
 *     [0]
 *        -> [1]
 *           -> (3): H
 *              -> (4): G
 *        -> (3)
 *           -> (4): C
 *              -> (7): A, D
 *        -> (4)
 *           -> (7): B
 *     [1]
 *        -> (5)
 *           -> (6): F
 *        -> (7): E
 * </pre>
 */
public class SchemaDescriptorLookupSet<T extends SchemaDescriptorSupplier>
{
    private final MutableIntObjectMap<EntityMultiSet> byFirstEntityToken = IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<PropertyMultiSet> byAnyEntityToken = IntObjectMaps.mutable.empty();

    /**
     * @return whether or not this set is empty, i.e. {@code true} if no descriptors have been added.
     */
    boolean isEmpty()
    {
        return byFirstEntityToken.isEmpty();
    }

    /**
     * Cheap way of finding out whether or not there are any descriptors matching the set of entity token ids and the property key id.
     *
     * @param entityTokenIds complete list of entity token ids for the entity to check.
     * @param propertyKey a property key id to check.
     * @return {@code true} if there are one or more descriptors matching the given tokens.
     */
    boolean has( long[] entityTokenIds, int propertyKey )
    {
        // Abort right away if there are no descriptors at all
        if ( isEmpty() )
        {
            return false;
        }

        // Check if there are any descriptors that matches any of the first (or only) entity token
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            EntityMultiSet first = byFirstEntityToken.get( toIntExact( entityTokenIds[i] ) );
            if ( first != null && first.has( entityTokenIds, i, propertyKey ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Cheap way of finding out whether or not there are any descriptors matching the given entity token id.
     *
     * @param entityTokenId entity token id to check.
     * @return {@code true} if there are one or more descriptors matching the given entity token.
     */
    boolean has( int entityTokenId )
    {
        return byAnyEntityToken.containsKey( entityTokenId );
    }

    /**
     * Adds the given descriptor to this set so that it can be looked up from any of the lookup methods.
     *
     * @param schemaDescriptor the descriptor to add.
     */
    public void add( T schemaDescriptor )
    {
        int[] entityTokenIds = sortedEntityTokenIds( schemaDescriptor.schema() );
        int firstEntityTokenId = entityTokenIds[0];
        byFirstEntityToken.getIfAbsentPut( firstEntityTokenId, EntityMultiSet::new ).add( schemaDescriptor, entityTokenIds, 0 );

        for ( int entityTokenId : entityTokenIds )
        {
            byAnyEntityToken.getIfAbsentPut( entityTokenId, PropertyMultiSet::new ).add( schemaDescriptor );
        }
    }

    /**
     * Removes the given descriptor from this set so that it can no longer be looked up from the lookup methods.
     * This operation is idempotent.
     *
     * @param schemaDescriptor the descriptor to remove.
     */
    public void remove( T schemaDescriptor )
    {
        int[] entityTokenIds = sortedEntityTokenIds( schemaDescriptor.schema() );
        int firstEntityTokenId = entityTokenIds[0];
        EntityMultiSet first = byFirstEntityToken.get( firstEntityTokenId );
        if ( first != null && first.remove( schemaDescriptor, entityTokenIds, 0 ) )
        {
            byFirstEntityToken.remove( firstEntityTokenId );
        }

        for ( int entityTokenId : entityTokenIds )
        {
            PropertyMultiSet any = byAnyEntityToken.get( entityTokenId );
            if ( any != null && any.remove( schemaDescriptor ) )
            {
                byAnyEntityToken.remove( entityTokenId );
            }
        }
    }

    /**
     * Collects descriptors matching the given complete list of entity tokens and property key tokens.
     * I.e. all tokens of the matching descriptors can be found in the given lists of tokens.
     *
     * @param into {@link Collection} to add matching descriptors into.
     * @param entityTokenIds complete and sorted array of entity token ids for the entity.
     * @param sortedProperties complete and sorted array of property key token ids for the entity.
     */
    void matchingDescriptorsForCompleteListOfProperties( Collection<T> into, long[] entityTokenIds, int[] sortedProperties )
    {
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            int entityTokenId = toIntExact( entityTokenIds[i] );
            EntityMultiSet first = byFirstEntityToken.get( entityTokenId );
            if ( first != null )
            {
                first.collectForCompleteListOfProperties( into, entityTokenIds, i, sortedProperties );
            }
        }
    }

    /**
     * Collects descriptors matching the given complete list of entity tokens, but only partial list of property key tokens.
     * I.e. all entity tokens of the matching descriptors can be found in the given lists of tokens,
     * but some matching descriptors may have other property key tokens in addition to those found in the given properties of the entity.
     * This is for a scenario where the complete list of property key tokens isn't known when calling this method and may
     * collect additional descriptors that in the end isn't relevant for the specific entity.
     *
     * @param into {@link Collection} to add matching descriptors into.
     * @param entityTokenIds complete and sorted array of entity token ids for the entity.
     * @param sortedProperties complete and sorted array of property key token ids for the entity.
     */
    void matchingDescriptorsForPartialListOfProperties( Collection<T> into, long[] entityTokenIds, int[] sortedProperties )
    {
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            int entityTokenId = toIntExact( entityTokenIds[i] );
            EntityMultiSet first = byFirstEntityToken.get( entityTokenId );
            if ( first != null )
            {
                first.collectForPartialListOfProperties( into, entityTokenIds, i, sortedProperties );
            }
        }
    }

    /**
     * Collects descriptors matching the given complete list of entity tokens.
     *
     * @param into {@link Collection} to add matching descriptors into.
     * @param entityTokenIds complete and sorted array of entity token ids for the entity.
     */
    void matchingDescriptors( Collection<T> into, long[] entityTokenIds )
    {
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            int entityTokenId = toIntExact( entityTokenIds[i] );
            EntityMultiSet set = byFirstEntityToken.get( entityTokenId );
            if ( set != null )
            {
                set.collectAll( into, entityTokenIds, i );
            }
        }
    }

    private class EntityMultiSet
    {
        private final PropertyMultiSet propertyMultiSet = new PropertyMultiSet();
        private final MutableIntObjectMap<EntityMultiSet> next = IntObjectMaps.mutable.empty();

        void add( T schemaDescriptor, int[] entityTokenIds, int cursor )
        {
            if ( cursor == entityTokenIds.length - 1 )
            {
                propertyMultiSet.add( schemaDescriptor );
            }
            else
            {
                int nextEntityTokenId = entityTokenIds[++cursor];
                next.getIfAbsentPut( nextEntityTokenId, EntityMultiSet::new ).add( schemaDescriptor, entityTokenIds, cursor );
            }
        }

        boolean remove( T schemaDescriptor, int[] entityTokenIds, int cursor )
        {
            if ( cursor == entityTokenIds.length - 1 )
            {
                propertyMultiSet.remove( schemaDescriptor );
            }
            else
            {
                int nextEntityTokenId = entityTokenIds[++cursor];
                EntityMultiSet nextSet = next.get( nextEntityTokenId );
                if ( nextSet != null && nextSet.remove( schemaDescriptor, entityTokenIds, cursor ) )
                {
                    next.remove( nextEntityTokenId );
                }
            }
            return propertyMultiSet.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties( Collection<T> into, long[] entityTokenIds, int entityTokenCursor, int[] sortedProperties )
        {
            propertyMultiSet.collectForCompleteListOfProperties( into, sortedProperties );

            if ( !next.isEmpty() )
            {
                for ( int i = entityTokenCursor + 1; i < entityTokenIds.length; i++ )
                {
                    int nextEntityTokenId = toIntExact( entityTokenIds[i] );
                    EntityMultiSet nextSet = next.get( nextEntityTokenId );
                    if ( nextSet != null )
                    {
                        nextSet.collectForCompleteListOfProperties( into, entityTokenIds, i, sortedProperties );
                    }
                }
            }
        }

        void collectForPartialListOfProperties( Collection<T> into, long[] entityTokenIds, int entityTokenCursor, int[] sortedProperties )
        {
            propertyMultiSet.collectForPartialListOfProperties( into, sortedProperties );

            if ( !next.isEmpty() )
            {
                for ( int i = entityTokenCursor + 1; i < entityTokenIds.length; i++ )
                {
                    int nextEntityTokenId = toIntExact( entityTokenIds[i] );
                    EntityMultiSet nextSet = next.get( nextEntityTokenId );
                    if ( nextSet != null )
                    {
                        nextSet.collectForPartialListOfProperties( into, entityTokenIds, i, sortedProperties );
                    }
                }
            }
        }

        void collectAll( Collection<T> into, long[] entityTokenIds, int entityTokenCursor )
        {
            propertyMultiSet.collectAll( into );

            if ( !next.isEmpty() )
            {
                for ( int i = entityTokenCursor + 1; i < entityTokenIds.length; i++ )
                {
                    int nextEntityTokenId = toIntExact( entityTokenIds[i] );
                    EntityMultiSet nextSet = next.get( nextEntityTokenId );
                    if ( nextSet != null )
                    {
                        nextSet.collectAll( into, entityTokenIds, i );
                    }
                }
            }
        }

        boolean has( long[] entityTokenIds, int entityTokenCursor, int propertyKey )
        {
            if ( propertyMultiSet.has( propertyKey ) )
            {
                return true;
            }
            if ( !next.isEmpty() )
            {
                for ( int i = entityTokenCursor + 1; i < entityTokenIds.length; i++ )
                {
                    int nextEntityTokenId = toIntExact( entityTokenIds[i] );
                    EntityMultiSet nextSet = next.get( nextEntityTokenId );
                    if ( nextSet != null && nextSet.has( entityTokenIds, i, propertyKey ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * A starting point for traversal of property key tokens from an entity token leaf.
     * Contains starting points of property key ids chains as well as lookup by any property in the chain.
     * Roughly like this:
     *
     * <pre>
     *     Descriptors:
     *     A: [0](5, 7)
     *     B: [0, 1](5, 4)
     *     C: [0, 1](4)
     *     D: [1, 0](6)
     *
     *     Data structure:
     *     [0]
     *        -> [1]
     *           -> (4): C
     *              -> (5): B
     *           -> (6): D
     *        -> (5)
     *           -> (7): A
     * </pre>
     *
     * In the above layout the [0] and [0] -> [1] are entity token "leaves" which each have a {@link PropertyMultiSet},
     * constituting the flip in the traversal between entity token chain and property chain.
     */
    private class PropertyMultiSet
    {
        private final Set<T> descriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();
        private final MutableIntObjectMap<Set<T>> byAnyProperty = IntObjectMaps.mutable.empty();

        void add( T schemaDescriptor )
        {
            // Add optimized path for when property list is fully known
            descriptors.add( schemaDescriptor );
            int[] propertyKeyIds = sortedPropertyKeyIds( schemaDescriptor.schema() );
            int propertyKeyId = propertyKeyIds[0];
            next.getIfAbsentPut( propertyKeyId, PropertySet::new ).add( schemaDescriptor, propertyKeyIds, 0 );

            // Add fall-back path for when property list is only partly known
            for ( int keyId : propertyKeyIds )
            {
                byAnyProperty.getIfAbsentPut( keyId, HashSet::new ).add( schemaDescriptor );
            }
        }

        /**
         * Removes the {@link SchemaDescriptor} from this multi-set.
         * @param schemaDescriptor the {@link SchemaDescriptor} to remove.
         * @return {@code true} if this multi-set ended up empty after removing this descriptor.
         */
        boolean remove( T schemaDescriptor )
        {
            // Remove from the optimized path
            descriptors.remove( schemaDescriptor );
            int[] propertyKeyIds = sortedPropertyKeyIds( schemaDescriptor.schema() );
            int propertyKeyId = propertyKeyIds[0];
            PropertySet firstPropertySet = next.get( propertyKeyId );
            if ( firstPropertySet != null && firstPropertySet.remove( schemaDescriptor, propertyKeyIds, 0 ) )
            {
                next.remove( propertyKeyId );
            }

            // Remove from the fall-back path
            for ( int keyId : propertyKeyIds )
            {
                Set<T> byProperty = byAnyProperty.get( keyId );
                if ( byProperty != null )
                {
                    byProperty.remove( schemaDescriptor );
                    if ( byProperty.isEmpty() )
                    {
                        byAnyProperty.remove( keyId );
                    }
                }
            }
            return descriptors.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties( Collection<T> descriptors, int[] sortedProperties )
        {
            for ( int i = 0; i < sortedProperties.length; i++ )
            {
                PropertySet firstSet = next.get( sortedProperties[i] );
                if ( firstSet != null )
                {
                    firstSet.collectForCompleteListOfProperties( descriptors, sortedProperties, i );
                }
            }
        }

        void collectForPartialListOfProperties( Collection<T> descriptors, int[] sortedProperties )
        {
            for ( int propertyKeyId : sortedProperties )
            {
                Set<T> propertyDescriptors = byAnyProperty.get( propertyKeyId );
                if ( propertyDescriptors != null )
                {
                    descriptors.addAll( propertyDescriptors );
                }
            }
        }

        void collectAll( Collection<T> descriptors )
        {
            descriptors.addAll( this.descriptors );
        }

        boolean has( int propertyKey )
        {
            return byAnyProperty.containsKey( propertyKey );
        }

        boolean isEmpty()
        {
            return descriptors.isEmpty() && next.isEmpty();
        }
    }

    /**
     * A single item in a property key chain. Sort of a subset, or a more specific version, of {@link PropertyMultiSet}.
     * It has a set containing descriptors that have their property chain end with this property key token and next pointers
     * to other property key tokens in known chains, but no way of looking up descriptors by any part of the chain,
     * like {@link PropertyMultiSet} has.
     */
    private class PropertySet
    {
        private final Set<T> fullDescriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();

        void add( T schemaDescriptor, int[] propertyKeyIds, int cursor )
        {
            if ( cursor == propertyKeyIds.length - 1 )
            {
                fullDescriptors.add( schemaDescriptor );
            }
            else
            {
                int nextPropertyKeyId = propertyKeyIds[++cursor];
                next.getIfAbsentPut( nextPropertyKeyId, PropertySet::new ).add( schemaDescriptor, propertyKeyIds, cursor );
            }
        }

        /**
         * @param schemaDescriptor {@link SchemaDescriptor} to remove.
         * @param propertyKeyIds the sorted property key ids for this schema.
         * @param cursor which property key among the sorted property keys that this set deals with.
         * @return {@code true} if this {@link PropertySet} ends up empty after this removal.
         */
        boolean remove( T schemaDescriptor, int[] propertyKeyIds, int cursor )
        {
            if ( cursor == propertyKeyIds.length - 1 )
            {
                fullDescriptors.remove( schemaDescriptor );
            }
            else
            {
                int nextPropertyKeyId = propertyKeyIds[++cursor];
                PropertySet propertySet = next.get( nextPropertyKeyId );
                if ( propertySet != null && propertySet.remove( schemaDescriptor, propertyKeyIds, cursor ) )
                {
                    next.remove( nextPropertyKeyId );
                }
            }
            return fullDescriptors.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties( Collection<T> descriptors, int[] sortedProperties, int cursor )
        {
            descriptors.addAll( fullDescriptors );
            if ( !next.isEmpty() )
            {
                for ( int i = cursor + 1; i < sortedProperties.length; i++ )
                {
                    PropertySet nextSet = next.get( sortedProperties[i] );
                    if ( nextSet != null )
                    {
                        nextSet.collectForCompleteListOfProperties( descriptors, sortedProperties, i );
                    }
                }
            }
        }
    }

    private static int[] sortedPropertyKeyIds( SchemaDescriptor schemaDescriptor )
    {
        return sortedTokenIds( schemaDescriptor.getPropertyIds() );
    }

    private static int[] sortedEntityTokenIds( SchemaDescriptor schemaDescriptor )
    {
        return sortedTokenIds( schemaDescriptor.getEntityTokenIds() );
    }

    private static int[] sortedTokenIds( int[] tokenIds )
    {
        if ( tokenIds.length > 1 )
        {
            // Clone it because we don't know if the array was an internal array that the descriptor handed out
            tokenIds = tokenIds.clone();
            Arrays.sort( tokenIds );
        }
        return tokenIds;
    }
}
