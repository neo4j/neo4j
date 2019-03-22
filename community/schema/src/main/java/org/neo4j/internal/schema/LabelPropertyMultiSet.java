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
 *     Descriptors
 *     A: label[0]properties[4, 7, 3]
 *     B: label[0]properties[7, 4]
 *     C: label[0]properties[3, 4]
 *     D: label[0]properties[3, 4, 7]
 *     E: label[1]properties[7]
 *     F: label[1]properties[5, 6]
 *
 *     Will result in a data structure (for the optimized path):
 *     label[0]
 *        -> property[3]
 *           -> property[4]: C
 *              -> property[7]: A, D
 *        -> property[4]
 *           -> property[7]: B
 *     label[1]
 *        -> property[5]
 *           -> property[6]: F
 *        -> property[7]: E
 * </pre>
 */
public class LabelPropertyMultiSet
{
    private final MutableIntObjectMap<PropertyMultiSet> byLabel = IntObjectMaps.mutable.empty();

    public void add( SchemaDescriptor schemaDescriptor )
    {
        if ( schemaDescriptor.getEntityTokenIds().length > 1 )
        {
            throw new UnsupportedOperationException();
        }

        int label = schemaDescriptor.keyId();
        PropertyMultiSet byProperty = byLabel.get( label );
        if ( byProperty == null )
        {
            byProperty = new PropertyMultiSet();
            byLabel.put( label, byProperty );
        }
        byProperty.add( schemaDescriptor );
    }

    public void remove( SchemaDescriptor schemaDescriptor )
    {
        int label = schemaDescriptor.keyId();
        PropertyMultiSet byProperty = byLabel.get( label );
        if ( byProperty != null && byProperty.remove( schemaDescriptor ) )
        {
            byLabel.remove( label );
        }
    }

    public void matchingDescriptorsForCompleteListOfProperties( Collection<SchemaDescriptor> into, long[] labels, int[] sortedProperties )
    {
        for ( long label : labels )
        {
            PropertyMultiSet byProperty = byLabel.get( (int) label );
            if ( byProperty != null )
            {
                byProperty.collectForCompleteListOfProperties( into, sortedProperties );
            }
        }
    }

    public void matchingDescriptorsForPartialListOfProperties( Collection<SchemaDescriptor> into, long[] labels, int[] sortedProperties )
    {
        for ( long label : labels )
        {
            PropertyMultiSet byProperty = byLabel.get( (int) label );
            if ( byProperty != null )
            {
                byProperty.collectForPartialListOfProperties( into, sortedProperties );
            }
        }
    }

    public void matchingDescriptors( Collection<SchemaDescriptor> into, long[] labels )
    {
        for ( long label : labels )
        {
            PropertyMultiSet set = byLabel.get( (int) label );
            if ( set != null )
            {
                set.collectAll( into );
            }
        }
    }

    private static class PropertyMultiSet
    {
        private final Set<SchemaDescriptor> descriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();
        private final MutableIntObjectMap<Set<SchemaDescriptor>> byAnyProperty = IntObjectMaps.mutable.empty();

        void add( SchemaDescriptor schemaDescriptor )
        {
            // Add optimized path for when property list is fully known
            descriptors.add( schemaDescriptor );
            int[] propertyKeyIds = sortedPropertyKeyIds( schemaDescriptor );
            int propertyKeyId = propertyKeyIds[0];
            PropertySet firstPropertySet = next.get( propertyKeyId );
            if ( firstPropertySet == null )
            {
                firstPropertySet = new PropertySet( propertyKeyId );
                next.put( propertyKeyId, firstPropertySet );
            }
            firstPropertySet.add( schemaDescriptor, propertyKeyIds, 0 );

            // Add fall-back path for when property list is only partly known
            for ( int keyId : propertyKeyIds )
            {
                Set<SchemaDescriptor> byProperty = byAnyProperty.get( keyId );
                if ( byProperty == null )
                {
                    byProperty = new HashSet<>();
                    byAnyProperty.put( keyId, byProperty );
                }
                byProperty.add( schemaDescriptor );
            }
        }

        /**
         * Removes the {@link SchemaDescriptor} from this multi-set.
         * @param schemaDescriptor the {@link SchemaDescriptor} to remove.
         * @return {@code true} if this multi-set ended up empty after removing this descriptor.
         */
        boolean remove( SchemaDescriptor schemaDescriptor )
        {
            // Remove from the optimized path
            descriptors.remove( schemaDescriptor );
            int[] propertyKeyIds = sortedPropertyKeyIds( schemaDescriptor );
            int propertyKeyId = propertyKeyIds[0];
            PropertySet firstPropertySet = next.get( propertyKeyId );
            if ( firstPropertySet != null && firstPropertySet.remove( schemaDescriptor, propertyKeyIds, 0 ) )
            {
                next.remove( propertyKeyId );
            }

            // Remove from the fall-back path
            for ( int keyId : propertyKeyIds )
            {
                Set<SchemaDescriptor> byProperty = byAnyProperty.get( keyId );
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

        void collectForCompleteListOfProperties( Collection<SchemaDescriptor> descriptors, int[] sortedProperties )
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

        void collectForPartialListOfProperties( Collection<SchemaDescriptor> descriptors, int[] sortedProperties )
        {
            for ( int propertyKeyId : sortedProperties )
            {
                Set<SchemaDescriptor> propertyDescriptors = byAnyProperty.get( propertyKeyId );
                if ( propertyDescriptors != null )
                {
                    descriptors.addAll( propertyDescriptors );
                }
            }
        }

        void collectAll( Collection<SchemaDescriptor> descriptors )
        {
            descriptors.addAll( this.descriptors );
        }

        private static int[] sortedPropertyKeyIds( SchemaDescriptor schemaDescriptor )
        {
            int[] propertyKeyIds = schemaDescriptor.getPropertyIds();
            if ( propertyKeyIds.length > 1 )
            {
                propertyKeyIds = propertyKeyIds.clone();
                Arrays.sort( propertyKeyIds );
            }
            return propertyKeyIds;
        }
    }

    private static class PropertySet
    {
        private final int propertyKey;
        private final Set<SchemaDescriptor> fullDescriptors = new HashSet<>();
        private final MutableIntObjectMap<PropertySet> next = IntObjectMaps.mutable.empty();

        PropertySet( int propertyKey )
        {
            this.propertyKey = propertyKey;
        }

        void add( SchemaDescriptor schemaDescriptor, int[] propertyKeyIds, int cursor )
        {
            if ( cursor == propertyKeyIds.length - 1 )
            {
                fullDescriptors.add( schemaDescriptor );
            }
            else
            {
                int nextPropertyKeyId = propertyKeyIds[++cursor];
                PropertySet propertySet = next.get( nextPropertyKeyId );
                if ( propertySet == null )
                {
                    propertySet = new PropertySet( nextPropertyKeyId );
                    next.put( nextPropertyKeyId, propertySet );
                }
                propertySet.add( schemaDescriptor, propertyKeyIds, cursor );
            }
        }

        /**
         * @param schemaDescriptor {@link SchemaDescriptor} to remove.
         * @param propertyKeyIds the sorted property key ids for this schema.
         * @param cursor which property key among the sorted property keys that this set deals with.
         * @return {@code true} if this {@link PropertySet} ends up empty after this removal.
         */
        boolean remove( SchemaDescriptor schemaDescriptor, int[] propertyKeyIds, int cursor )
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

        void collectForCompleteListOfProperties( Collection<SchemaDescriptor> descriptors, int[] sortedProperties, int cursor )
        {
            assert sortedProperties[cursor] == propertyKey;
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
