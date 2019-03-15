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

import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final MutableLongObjectMap<IndexProxy> indexesById;
    private final Map<SchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor;
    private final LabelPropertyMultiSet descriptorsByLabelThenProperty;
    private final LabelPropertyMultiSet descriptorsByReltypeThenProperty;

    public IndexMap()
    {
        this( new LongObjectHashMap<>(), new HashMap<>(), new ObjectLongHashMap<>() );
    }

    IndexMap( MutableLongObjectMap<IndexProxy> indexesById )
    {
        this( indexesById, indexesByDescriptor( indexesById ), indexIdsByDescriptor( indexesById ) );
    }

    private IndexMap(
            MutableLongObjectMap<IndexProxy> indexesById,
            Map<SchemaDescriptor,IndexProxy> indexesByDescriptor,
            MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.descriptorsByLabelThenProperty = new LabelPropertyMultiSet();
        this.descriptorsByReltypeThenProperty = new LabelPropertyMultiSet();
        for ( SchemaDescriptor schema : indexesByDescriptor.keySet() )
        {
            addDescriptorToLookups( schema );
        }
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( SchemaDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public long getIndexId( SchemaDescriptor descriptor )
    {
        return indexIdsByDescriptor.get( descriptor );
    }

    public void putIndexProxy( IndexProxy indexProxy )
    {
        StoreIndexDescriptor descriptor = indexProxy.getDescriptor();
        SchemaDescriptor schema = descriptor.schema();
        indexesById.put( descriptor.getId(), indexProxy );
        indexesByDescriptor.put( schema, indexProxy );
        indexIdsByDescriptor.put( schema, descriptor.getId() );
        addDescriptorToLookups( schema );
    }

    IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( removedProxy == null )
        {
            return null;
        }

        SchemaDescriptor schema = removedProxy.getDescriptor().schema();
        indexesByDescriptor.remove( schema );
        if ( schema.entityType() == EntityType.NODE )
        {
            descriptorsByLabelThenProperty.remove( schema );
        }
        else if ( schema.entityType() == EntityType.RELATIONSHIP )
        {
            descriptorsByReltypeThenProperty.remove( schema );
        }

        return removedProxy;
    }

    void forEachIndexProxy( LongObjectProcedure<IndexProxy> consumer )
    {
        indexesById.forEachKeyValue( consumer );
    }

    Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexesById.values();
    }

    /**
     * Get all indexes that would be affected by changes in the input labels and/or properties. The returned
     * indexes are guaranteed to contain all affected indexes, but might also contain unaffected indexes as
     * we cannot provide matching without checking unaffected properties for composite indexes.
     *
     * @param changedEntityTokens set of labels that have changed
     * @param unchangedEntityTokens set of labels that are unchanged
     * @param sortedProperties sorted list of properties
     * @param entityType type of indexes to get
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    public Set<SchemaDescriptor> getRelatedIndexes( long[] changedEntityTokens, long[] unchangedEntityTokens, int[] sortedProperties,
            boolean propertyListIsComplete, EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return getRelatedDescriptors( changedEntityTokens, unchangedEntityTokens, sortedProperties, propertyListIsComplete,
                    descriptorsByLabelThenProperty );
        case RELATIONSHIP:
            return getRelatedDescriptors( changedEntityTokens, unchangedEntityTokens, sortedProperties, propertyListIsComplete,
                    descriptorsByReltypeThenProperty );
        default:
            throw new IllegalArgumentException( "The given EntityType cannot be indexed: " + entityType );
        }
    }

    /**
     * @param changedLabels set of labels that have changed
     * @param unchangedLabels set of labels that are unchanged
     * @param sortedProperties set of properties
     * @param propertyListIsComplete whether or not the property list is complete. For CREATE/DELETE the list is complete, but may not be for UPDATEs.
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    Set<SchemaDescriptor> getRelatedDescriptors( long[] changedLabels, long[] unchangedLabels, int[] sortedProperties, boolean propertyListIsComplete,
            LabelPropertyMultiSet set )
    {
        if ( indexesById.isEmpty() )
        {
            return Collections.emptySet();
        }

        Set<SchemaDescriptor> descriptors = new HashSet<>();
        if ( propertyListIsComplete )
        {
            set.matchingDescriptorsForCompleteListOfProperties( descriptors, changedLabels, sortedProperties );
        }
        else
        {
            // At the time of writing this the commit process won't load the complete list of property keys for an entity.
            // Because of this the matching cannot be as precise as if the complete list was known.
            // Anyway try to make the best out of it and narrow down the list of potentially related indexes as much as possible.
            if ( sortedProperties.length == 0 )
            {
                // Only labels changed. Since we don't know which properties this entity has let's include all indexes for the changed labels.
                set.matchingDescriptors( descriptors, changedLabels );
            }
            else if ( changedLabels.length == 0 )
            {
                // Only properties changed. Since we don't know which other properties this entity has let's include all indexes
                // for the (unchanged) labels on this entity that has any match on any of the changed properties.
                set.matchingDescriptorsForPartialListOfProperties( descriptors, unchangedLabels, sortedProperties );
            }
            else
            {
                // Both labels and properties changed.
                // All indexes for the changed labels must be included.
                // Also include all indexes for any of the changed or unchanged labels that has any match on any of the changed properties.
                set.matchingDescriptors( descriptors, changedLabels );
                set.matchingDescriptorsForPartialListOfProperties( descriptors, unchangedLabels, sortedProperties );
            }
        }
        return descriptors;
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( LongObjectHashMap.newMap( indexesById ), cloneMap( indexesByDescriptor ), new ObjectLongHashMap<>( indexIdsByDescriptor ) );
    }

    public Iterator<SchemaDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public LongIterator indexIds()
    {
        return indexesById.keySet().longIterator();
    }

    public int size()
    {
        return indexesById.size();
    }

    // HELPERS

    private <K, V> Map<K, V> cloneMap( Map<K, V> map )
    {
        Map<K, V> shallowCopy = new HashMap<>( map.size() );
        shallowCopy.putAll( map );
        return shallowCopy;
    }

    private void addDescriptorToLookups( SchemaDescriptor schema )
    {
        if ( schema.entityType() == EntityType.NODE )
        {
            descriptorsByLabelThenProperty.add( schema );
        }
        else if ( schema.entityType() == EntityType.RELATIONSHIP )
        {
            descriptorsByReltypeThenProperty.add( schema );
        }
    }

    private static Map<SchemaDescriptor, IndexProxy> indexesByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        return indexesById.toMap( indexProxy -> indexProxy.getDescriptor().schema(), Functions.identity() );
    }

    private static MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        final MutableObjectLongMap<SchemaDescriptor> map = new ObjectLongHashMap<>( indexesById.size() );
        indexesById.forEachKeyValue( ( id, indexProxy ) -> map.put( indexProxy.getDescriptor().schema(), id ) );
        return map;
    }
}
