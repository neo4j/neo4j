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

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Math.toIntExact;

/**
 * Collects and provides efficient access to {@link SchemaDescriptor}, based on label ids and partial or full list of properties.
 * The descriptors are first grouped by label id and then for a specific label id they are further grouped by property id.
 * The property grouping works on sorted property key id lists as to minimize deduplication when selecting for composite indexes.
 * <p>
 * The selection works efficiently when providing complete list of properties, but will have to resort to some amount of over-selection and union
 * when caller can only provide partial list of properties. The most efficient linking starts from label id and traverses through the property key ids
 * of the schema descriptor (single or multiple for composite index) in sorted order where each property key id (for this label) links to the next
 * property key. From all leaves along the way the descriptors can be collected. This way only the actually matching indexes will be collected
 * instead of collecting all indexes matching any property key and doing a union of those. Example:
 *
 * <pre>
 *     Legend: ids inside [] are labels, ids inside () are properties
 *     Descriptors
 *     A: [0](4, 7, 3)
 *     B: [0](7, 4)
 *     C: [0](3, 4)
 *     D: [0](3, 4, 7)
 *     E: [1](7)
 *     F: [1](5, 6)
 *     TODO add multi-entity descriptors
 *
 *     Will result in a data structure (for the optimized path):
 *     [0]
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
public class LabelPropertyMultiSet<T extends SchemaDescriptorSupplier>
{
    private final MutableIntObjectMap<LabelMultiSet> byFirstLabel = IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<PropertyMultiSet> byAnyLabel = IntObjectMaps.mutable.empty();

    boolean isEmpty()
    {
        return byFirstLabel.isEmpty();
    }

    public boolean has( long[] labels, int propertyKey )
    {
        if ( byFirstLabel.isEmpty() )
        {
            return false;
        }

        for ( int i = 0; i < labels.length; i++ )
        {
            LabelMultiSet labelMultiSet = byFirstLabel.get( toIntExact( labels[i] ) );
            if ( labelMultiSet != null && labelMultiSet.has( labels, i, propertyKey ) )
            {
                return true;
            }
        }
        return false;
    }

    public boolean has( int label )
    {
        return !byAnyLabel.isEmpty() && byAnyLabel.containsKey( label );
    }

    public void add( T schemaDescriptor )
    {
        int[] entityTokenIds = schemaDescriptor.schema().getEntityTokenIds();
        int firstEntityTokenId = entityTokenIds[0];
        byFirstLabel.getIfAbsentPut( firstEntityTokenId, LabelMultiSet::new ).add( schemaDescriptor, entityTokenIds, 0 );

        for ( int entityTokenId : entityTokenIds )
        {
            byAnyLabel.getIfAbsentPut( entityTokenId, PropertyMultiSet::new ).add( schemaDescriptor );
        }
    }

    public void remove( T schemaDescriptor )
    {
        int[] entityTokenIds = schemaDescriptor.schema().getEntityTokenIds();
        int firstEntityTokenId = entityTokenIds[0];
        LabelMultiSet labelMultiSet = byFirstLabel.get( firstEntityTokenId );
        if ( labelMultiSet != null && labelMultiSet.remove( schemaDescriptor, entityTokenIds, 0 ) )
        {
            byFirstLabel.remove( firstEntityTokenId );
        }

        for ( int entityTokenId : entityTokenIds )
        {
            PropertyMultiSet propertyMultiSet = byAnyLabel.get( entityTokenId );
            if ( propertyMultiSet != null && propertyMultiSet.remove( schemaDescriptor ) )
            {
                byAnyLabel.remove( entityTokenId );
            }
        }
    }

    public void matchingDescriptorsForCompleteListOfProperties( Collection<T> into, long[] labels, int[] sortedProperties )
    {
        for ( int i = 0; i < labels.length; i++ )
        {
            int label = (int) labels[i];
            LabelMultiSet labelMultiSet = byFirstLabel.get( label );
            if ( labelMultiSet != null )
            {
                labelMultiSet.collectForCompleteListOfProperties( into, labels, i, sortedProperties );
            }
        }
    }

    public void matchingDescriptorsForPartialListOfProperties( Collection<T> into, long[] labels, int[] sortedProperties )
    {
        for ( int i = 0; i < labels.length; i++ )
        {
            int label = (int) labels[i];
            LabelMultiSet labelMultiSet = byFirstLabel.get( label );
            if ( labelMultiSet != null )
            {
                labelMultiSet.collectForPartialListOfProperties( into, labels, i, sortedProperties );
            }
        }
    }

    public void matchingDescriptors( Collection<T> into, long[] labels )
    {
        for ( int i = 0; i < labels.length; i++ )
        {
            int label = (int) labels[i];
            LabelMultiSet set = byFirstLabel.get( label );
            if ( set != null )
            {
                set.collectAll( into, labels, i );
            }
        }
    }

    private class LabelMultiSet
    {
        private final PropertyMultiSet propertyMultiSet = new PropertyMultiSet();
        private final MutableIntObjectMap<LabelMultiSet> next = IntObjectMaps.mutable.empty();

        void add( T schemaDescriptor, int[] entityTokenIds, int cursor )
        {
            if ( cursor == entityTokenIds.length - 1 )
            {
                propertyMultiSet.add( schemaDescriptor );
            }
            else
            {
                int nextEntityTokenId = entityTokenIds[++cursor];
                next.getIfAbsentPut( nextEntityTokenId, LabelMultiSet::new ).add( schemaDescriptor, entityTokenIds, cursor );
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
                LabelMultiSet nextSet = next.get( nextEntityTokenId );
                if ( nextSet != null && nextSet.remove( schemaDescriptor, entityTokenIds, cursor ) )
                {
                    next.remove( nextEntityTokenId );
                }
            }
            return propertyMultiSet.isEmpty() && next.isEmpty();
        }

        void collectForCompleteListOfProperties( Collection<T> into, long[] labels, int labelsCursor, int[] sortedProperties )
        {
            propertyMultiSet.collectForCompleteListOfProperties( into, sortedProperties );
            // TODO potentially optimize be checking if there even are any next at all before looping? Same thing could be done in PropertyMultiSet
            for ( int i = labelsCursor + 1; i < labels.length; i++ )
            {
                int nextLabel = (int) labels[i];
                LabelMultiSet nextLabelMultiSet = next.get( nextLabel );
                if ( nextLabelMultiSet != null )
                {
                    nextLabelMultiSet.collectForCompleteListOfProperties( into, labels, i, sortedProperties );
                }
            }
        }

        void collectForPartialListOfProperties( Collection<T> into, long[] labels, int labelsCursor, int[] sortedProperties )
        {
            propertyMultiSet.collectForPartialListOfProperties( into, sortedProperties );
            // TODO potentially optimize be checking if there even are any next at all before looping? Same thing could be done in PropertyMultiSet
            for ( int i = labelsCursor + 1; i < labels.length; i++ )
            {
                int nextLabel = (int) labels[i];
                LabelMultiSet nextLabelMultiSet = next.get( nextLabel );
                if ( nextLabelMultiSet != null )
                {
                    nextLabelMultiSet.collectForPartialListOfProperties( into, labels, i, sortedProperties );
                }
            }
        }

        void collectAll( Collection<T> into, long[] labels, int labelsCursor )
        {
            propertyMultiSet.collectAll( into );
            for ( int i = labelsCursor + 1; i < labels.length; i++ )
            {
                int nextLabel = (int) labels[i];
                LabelMultiSet nextLabelMultiSet = next.get( nextLabel );
                if ( nextLabelMultiSet != null )
                {
                    nextLabelMultiSet.collectAll( into, labels, i );
                }
            }
        }

        boolean has( long[] labels, int labelsCursor, int propertyKey )
        {
            if ( propertyMultiSet.has( propertyKey ) )
            {
                return true;
            }
            for ( int i = labelsCursor + 1; i < labels.length; i++ )
            {
                int nextLabel = (int) labels[i];
                LabelMultiSet nextLabelMultiSet = next.get( nextLabel );
                if ( nextLabelMultiSet != null && nextLabelMultiSet.has( labels, i, propertyKey ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

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

        private int[] sortedPropertyKeyIds( SchemaDescriptor schemaDescriptor )
        {
            int[] propertyKeyIds = schemaDescriptor.getPropertyIds();
            if ( propertyKeyIds.length > 1 )
            {
                propertyKeyIds = propertyKeyIds.clone();
                Arrays.sort( propertyKeyIds );
            }
            return propertyKeyIds;
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
