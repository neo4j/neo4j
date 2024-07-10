/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.schema;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.COMPLETE_ALL_TOKENS;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.PARTIAL_ANY_TOKEN;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

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
 *     Single-entity token descriptors
 *     A: [0](4, 7, 3)
 *     B: [0](7, 4)
 *     C: [0](3, 4)
 *     D: [0](3, 4, 7)
 *     E: [1](7)
 *     F: [1](5, 6)
 *     Multi-entity token descriptors (matches are made on any of the entity/property key tokens)
 *     G: [0, 1](3, 4)
 *     H: [1, 0](3)
 *
 *     Will result in a data structure (for the optimized path):
 *     [0]
 *        -> (3): G, H
 *           -> (4): C
 *              -> (7): A, D
 *        -> (4): G
 *           -> (7): B
 *     [1]
 *        -> (3): G, H
 *        -> (4): G
 *        -> (5)
 *           -> (6): F
 *        -> (7): E
 * </pre>
 */
public class SchemaDescriptorLookupSet<T extends SchemaDescriptorSupplier> {
    private final MutableIntObjectMap<PropertyMultiSet> byEntityToken = IntObjectMaps.mutable.empty();

    /**
     * @return whether or not this set is empty, i.e. {@code true} if no descriptors have been added.
     */
    public boolean isEmpty() {
        return byEntityToken.isEmpty();
    }

    /**
     * Cheap way of finding out whether or not there are any descriptors matching the set of entity token ids and the property key id.
     *
     * @param entityTokenIds complete list of entity token ids for the entity to check.
     * @param propertyKey a property key id to check.
     * @return {@code true} if there are one or more descriptors matching the given tokens.
     */
    public boolean has(int[] entityTokenIds, int propertyKey) {
        // Abort right away if there are no descriptors at all
        if (isEmpty()) {
            return false;
        }

        // Check if there are any descriptors that matches any of the first (or only) entity token
        for (long entityTokenId : entityTokenIds) {
            PropertyMultiSet set = byEntityToken.get(toIntExact(entityTokenId));
            if (set != null && set.has(propertyKey)) {
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
    public boolean has(int entityTokenId) {
        return byEntityToken.containsKey(entityTokenId);
    }

    /**
     * Adds the given descriptor to this set so that it can be looked up from any of the lookup methods.
     *
     * @param schemaDescriptor the descriptor to add.
     */
    public void add(T schemaDescriptor) {
        for (int entityTokenId : schemaDescriptor.schema().getEntityTokenIds()) {
            byEntityToken.getIfAbsentPut(entityTokenId, PropertyMultiSet::new).add(schemaDescriptor);
        }
    }

    /**
     * Removes the given descriptor from this set so that it can no longer be looked up from the lookup methods.
     * This operation is idempotent.
     *
     * @param schemaDescriptor the descriptor to remove.
     */
    public void remove(T schemaDescriptor) {
        for (int entityTokenId : schemaDescriptor.schema().getEntityTokenIds()) {
            PropertyMultiSet any = byEntityToken.get(entityTokenId);
            if (any != null && any.remove(schemaDescriptor)) {
                byEntityToken.remove(entityTokenId);
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
    public void matchingDescriptorsForCompleteListOfProperties(
            Collection<T> into, int[] entityTokenIds, int[] sortedProperties) {
        for (long entityTokenId : entityTokenIds) {
            PropertyMultiSet first = byEntityToken.get(toIntExact(entityTokenId));
            if (first != null) {
                first.collectForCompleteListOfProperties(into, sortedProperties);
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
    public void matchingDescriptorsForPartialListOfProperties(
            Collection<T> into, int[] entityTokenIds, int[] sortedProperties) {
        for (long entityTokenId : entityTokenIds) {
            PropertyMultiSet first = byEntityToken.get(toIntExact(entityTokenId));
            if (first != null) {
                first.collectForPartialListOfProperties(into, sortedProperties);
            }
        }
    }

    /**
     * Collects descriptors matching the given complete list of entity tokens.
     *
     * @param into {@link Collection} to add matching descriptors into.
     * @param entityTokenIds complete and sorted array of entity token ids for the entity.
     */
    public void matchingDescriptors(Collection<T> into, int[] entityTokenIds) {
        for (long entityTokenId : entityTokenIds) {
            PropertyMultiSet set = byEntityToken.get(toIntExact(entityTokenId));
            if (set != null) {
                set.collectAll(into);
            }
        }
    }

    /**
     * A starting point for traversal of property key tokens. Contains starting points of property key id chains
     * as well as lookup by any property in the chain.
     * Roughly like this:
     *
     * <pre>
     *     Descriptors:
     *     A: (5, 7)
     *     B: (5, 4)
     *     C: (4)
     *     D: (6)
     *
     *     Data structure:
     *     (4): C
     *        -> (5): B
     *     (5)
     *        -> (7): A
     *     (6): D
     * </pre>
     */
    private class PropertyMultiSet {
        private final Set<T> descriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();
        private final MutableIntObjectMap<Set<T>> byAnyProperty = IntObjectMaps.mutable.empty();

        void add(T schemaDescriptor) {
            // Add optimized path for when property list is fully known
            descriptors.add(schemaDescriptor);
            int[] propertyKeyIds = sortedPropertyKeyIds(schemaDescriptor.schema());
            SchemaPatternMatchingType schemaPatternMatchingType =
                    schemaDescriptor.schema().schemaPatternMatchingType();
            if (schemaPatternMatchingType == COMPLETE_ALL_TOKENS) {
                // Just add the first token id to the top level set
                next.getIfAbsentPut(propertyKeyIds[0], PropertySet::new).add(schemaDescriptor, propertyKeyIds, 0);
            } else if (schemaPatternMatchingType == PARTIAL_ANY_TOKEN) {
                // The internal data structure is built and optimized for when all property key tokens are required to
                // match
                // a particular descriptor. However to support the partial type, where any property key may match
                // we will have to add such descriptors to all property key sets and pretend that each is the only one.
                for (int propertyKeyId : propertyKeyIds) {
                    next.getIfAbsentPut(propertyKeyId, PropertySet::new)
                            .add(schemaDescriptor, new int[] {propertyKeyId}, 0);
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unknown schema pattern matching type " + schemaPatternMatchingType);
            }

            // Add fall-back path for when property list is only partly known
            for (int keyId : propertyKeyIds) {
                byAnyProperty.getIfAbsentPut(keyId, HashSet::new).add(schemaDescriptor);
            }
        }

        /**
         * Removes the {@link SchemaDescriptor} from this multi-set.
         * @param schemaDescriptor the {@link SchemaDescriptor} to remove.
         * @return {@code true} if this multi-set ended up empty after removing this descriptor.
         */
        boolean remove(T schemaDescriptor) {
            // Remove from the optimized path
            descriptors.remove(schemaDescriptor);
            int[] propertyKeyIds = sortedPropertyKeyIds(schemaDescriptor.schema());
            SchemaPatternMatchingType schemaPatternMatchingType =
                    schemaDescriptor.schema().schemaPatternMatchingType();
            if (schemaPatternMatchingType == COMPLETE_ALL_TOKENS) {
                int firstPropertyKeyId = propertyKeyIds[0];
                PropertySet firstPropertySet = next.get(firstPropertyKeyId);
                if (firstPropertySet != null && firstPropertySet.remove(schemaDescriptor, propertyKeyIds, 0)) {
                    next.remove(firstPropertyKeyId);
                }
            } else if (schemaPatternMatchingType == PARTIAL_ANY_TOKEN) {
                for (int propertyKeyId : propertyKeyIds) {
                    PropertySet propertySet = next.get(propertyKeyId);
                    if (propertySet != null && propertySet.remove(schemaDescriptor, new int[] {propertyKeyId}, 0)) {
                        next.remove(propertyKeyId);
                    }
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unknown schema pattern matching type" + schemaPatternMatchingType);
            }

            // Remove from the fall-back path
            for (int keyId : propertyKeyIds) {
                Set<T> byProperty = byAnyProperty.get(keyId);
                if (byProperty != null) {
                    byProperty.remove(schemaDescriptor);
                    if (byProperty.isEmpty()) {
                        byAnyProperty.remove(keyId);
                    }
                }
            }
            return descriptors.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties(Collection<T> descriptors, int[] sortedProperties) {
            for (int i = 0; i < sortedProperties.length; i++) {
                PropertySet firstSet = next.get(sortedProperties[i]);
                if (firstSet != null) {
                    firstSet.collectForCompleteListOfProperties(descriptors, sortedProperties, i);
                }
            }
        }

        void collectForPartialListOfProperties(Collection<T> descriptors, int[] sortedProperties) {
            for (int propertyKeyId : sortedProperties) {
                Set<T> propertyDescriptors = byAnyProperty.get(propertyKeyId);
                if (propertyDescriptors != null) {
                    descriptors.addAll(propertyDescriptors);
                }
            }
        }

        void collectAll(Collection<T> descriptors) {
            descriptors.addAll(this.descriptors);
        }

        boolean has(int propertyKey) {
            return byAnyProperty.containsKey(propertyKey);
        }
    }

    /**
     * A single item in a property key chain. Sort of a subset, or a more specific version, of {@link PropertyMultiSet}.
     * It has a set containing descriptors that have their property chain end with this property key token and next pointers
     * to other property key tokens in known chains, but no way of looking up descriptors by any part of the chain,
     * like {@link PropertyMultiSet} has.
     */
    private class PropertySet {
        private final Set<T> fullDescriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();

        void add(T schemaDescriptor, int[] propertyKeyIds, int cursor) {
            if (cursor == propertyKeyIds.length - 1) {
                fullDescriptors.add(schemaDescriptor);
            } else {
                int nextPropertyKeyId = propertyKeyIds[++cursor];
                next.getIfAbsentPut(nextPropertyKeyId, PropertySet::new).add(schemaDescriptor, propertyKeyIds, cursor);
            }
        }

        /**
         * @param schemaDescriptor {@link SchemaDescriptor} to remove.
         * @param propertyKeyIds the sorted property key ids for this schema.
         * @param cursor which property key among the sorted property keys that this set deals with.
         * @return {@code true} if this {@link PropertySet} ends up empty after this removal.
         */
        boolean remove(T schemaDescriptor, int[] propertyKeyIds, int cursor) {
            if (cursor == propertyKeyIds.length - 1) {
                fullDescriptors.remove(schemaDescriptor);
            } else {
                int nextPropertyKeyId = propertyKeyIds[++cursor];
                PropertySet propertySet = next.get(nextPropertyKeyId);
                if (propertySet != null && propertySet.remove(schemaDescriptor, propertyKeyIds, cursor)) {
                    next.remove(nextPropertyKeyId);
                }
            }
            return fullDescriptors.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties(Collection<T> descriptors, int[] sortedProperties, int cursor) {
            descriptors.addAll(fullDescriptors);
            if (!next.isEmpty()) {
                for (int i = cursor + 1; i < sortedProperties.length; i++) {
                    PropertySet nextSet = next.get(sortedProperties[i]);
                    if (nextSet != null) {
                        nextSet.collectForCompleteListOfProperties(descriptors, sortedProperties, i);
                    }
                }
            }
        }
    }

    private static int[] sortedPropertyKeyIds(SchemaDescriptor schemaDescriptor) {
        int[] tokenIds = schemaDescriptor.getPropertyIds();
        if (tokenIds.length > 1) {
            // Clone it because we don't know if the array was an internal array that the descriptor handed out
            tokenIds = Arrays.copyOf(tokenIds, tokenIds.length);
            Arrays.sort(tokenIds);
        }
        return tokenIds;
    }
}
