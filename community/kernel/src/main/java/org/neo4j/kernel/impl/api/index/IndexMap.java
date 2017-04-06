/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final Map<Long, IndexProxy> indexesById;
    private final Map<LabelSchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final Map<LabelSchemaDescriptor,Long> indexIdsByDescriptor;
    private final PrimitiveIntObjectMap<Set<LabelSchemaDescriptor>> descriptorsByLabel;
    private final PrimitiveIntObjectMap<Set<LabelSchemaDescriptor>> descriptorsByProperty;

    public IndexMap()
    {
        this( new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }

    IndexMap( Map<Long,IndexProxy> indexesById )
    {
        this( indexesById, indexesByDescriptor( indexesById ), indexIdsByDescriptor( indexesById ) );
    }

    private IndexMap(
            Map<Long, IndexProxy> indexesById,
            Map<LabelSchemaDescriptor,IndexProxy> indexesByDescriptor,
            Map<LabelSchemaDescriptor,Long> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.descriptorsByLabel = Primitive.intObjectMap();
        this.descriptorsByProperty = Primitive.intObjectMap();
        for ( LabelSchemaDescriptor schema : indexesByDescriptor.keySet() )
        {
            addDescriptorToLookups( schema );
        }
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( LabelSchemaDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public long getIndexId( LabelSchemaDescriptor descriptor )
    {
        return indexIdsByDescriptor.get( descriptor );
    }

    public void putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        LabelSchemaDescriptor schema = indexProxy.getDescriptor().schema();
        indexesById.put( indexId, indexProxy );
        indexesByDescriptor.put( schema, indexProxy );
        indexIdsByDescriptor.put( schema, indexId );
        addDescriptorToLookups( schema );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( removedProxy == null )
        {
            return null;
        }

        LabelSchemaDescriptor schema = removedProxy.getDescriptor().schema();
        indexesByDescriptor.remove( schema );

        removeFromLookup( schema.getLabelId(), schema, descriptorsByLabel );
        for ( int propertyId : schema.getPropertyIds() )
        {
            removeFromLookup( propertyId, schema, descriptorsByProperty );
        }

        return removedProxy;
    }

    public void foreachIndexProxy( BiConsumer<Long, IndexProxy> consumer )
    {
        for ( Map.Entry<Long, IndexProxy> entry : indexesById.entrySet() )
        {
            consumer.accept( entry.getKey(), entry.getValue() );
        }
    }

    public Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexesById.values();
    }

    /**
     * Get all indexes that would be affected by changes in the input labels and/or properties. The returned
     * indexes are guaranteed to contain all affected indexes, but might also contain unaffected indexes as
     * we cannot provide matching without checking unaffected properties for composite indexes.
     *
     * @param changedLabels set of labels that have changed
     * @param unchangedLabels set of labels that are unchanged
     * @param properties set of properties
     * @return set of LabelSchemaDescriptors describing the potentially affected indexes
     */
    public Set<LabelSchemaDescriptor> getRelatedIndexes(
            long[] changedLabels, long[] unchangedLabels, PrimitiveIntCollection properties )
    {
        if ( changedLabels.length == 1 && properties.isEmpty() )
        {
            Set<LabelSchemaDescriptor> descriptors = descriptorsByLabel.get( (int)changedLabels[0] );
            return descriptors == null ? Collections.emptySet() : descriptors;
        }

        if ( changedLabels.length == 0 && properties.size() == 1 )
        {
            return getDescriptorsByProperties( unchangedLabels, properties );
        }

        Set<LabelSchemaDescriptor> descriptors = extractIndexesByLabels( changedLabels );
        descriptors.addAll( getDescriptorsByProperties( unchangedLabels, properties ) );

        return descriptors;
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( cloneMap( indexesById ), cloneMap( indexesByDescriptor ),
                cloneMap( indexIdsByDescriptor ) );
    }

    public Iterator<LabelSchemaDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public Iterator<Long> indexIds()
    {
        return indexesById.keySet().iterator();
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

    private void addDescriptorToLookups( LabelSchemaDescriptor schema )
    {
        addToLookup( schema.getLabelId(), schema, descriptorsByLabel );

        for ( int propertyId : schema.getPropertyIds() )
        {
            addToLookup( propertyId, schema, descriptorsByProperty );
        }
    }

    private void addToLookup(
            int key,
            LabelSchemaDescriptor schema,
            PrimitiveIntObjectMap<Set<LabelSchemaDescriptor>> lookup )
    {
        Set<LabelSchemaDescriptor> descriptors = lookup.get( key );
        if ( descriptors == null )
        {
            descriptors = new HashSet<>();
            lookup.put( key, descriptors );
        }
        descriptors.add( schema );
    }

    private void removeFromLookup(
            int key,
            LabelSchemaDescriptor schema,
            PrimitiveIntObjectMap<Set<LabelSchemaDescriptor>> lookup )
    {
        Set<LabelSchemaDescriptor> descriptors = lookup.get( key );
        descriptors.remove( schema );
        if ( descriptors.isEmpty() )
        {
            lookup.remove( key );
        }
    }

    private static Map<LabelSchemaDescriptor, IndexProxy> indexesByDescriptor( Map<Long,IndexProxy> indexesById )
    {
        Map<LabelSchemaDescriptor, IndexProxy> map = new HashMap<>();
        for ( IndexProxy proxy : indexesById.values() )
        {
            map.put( proxy.schema(), proxy );
        }
        return map;
    }

    private static Map<LabelSchemaDescriptor, Long> indexIdsByDescriptor( Map<Long,IndexProxy> indexesById )
    {
        Map<LabelSchemaDescriptor, Long> map = new HashMap<>();
        for ( Map.Entry<Long,IndexProxy> entry : indexesById.entrySet() )
        {
            map.put( entry.getValue().schema(), entry.getKey() );
        }
        return map;
    }

    /**
     * Get descriptors affected by changed properties. Implementation checks whether doing
     * the lookup using the unchanged labels or the changed properties given the smallest final
     * set of indexes, and chooses the best way.
     *
     * @param unchangedLabels set of labels that are unchanged
     * @param properties set of properties that have changed
     * @return set of LabelSchemaDescriptors describing the potentially affected indexes
     */
    private Set<LabelSchemaDescriptor> getDescriptorsByProperties(
            long[] unchangedLabels,
            PrimitiveIntCollection properties )
    {
        int nIndexesForLabels = countIndexesByLabels( unchangedLabels );
        int nIndexesForProperties = countIndexesByProperties( properties );

        if ( nIndexesForLabels == 0 || nIndexesForProperties == 0 )
        {
            return Collections.emptySet();
        }
        if ( nIndexesForLabels < nIndexesForProperties )
        {
            return extractIndexesByLabels( unchangedLabels );
        }
        else
        {
            return extractIndexesByProperties( properties );
        }
    }

    private Set<LabelSchemaDescriptor> extractIndexesByLabels( long[] labels )
    {
        Set<LabelSchemaDescriptor> set = new HashSet<>();
        for ( long label : labels )
        {
            Set<LabelSchemaDescriptor> forLabel = descriptorsByLabel.get( (int) label );
            if ( forLabel != null )
            {
                set.addAll( forLabel );
            }
        }
        return set;
    }

    private int countIndexesByLabels( long[] labels )
    {
        int count = 0;
        for ( long label : labels )
        {
            Set<LabelSchemaDescriptor> forLabel = descriptorsByLabel.get( (int) label );
            if ( forLabel != null )
            {
                count += forLabel.size();
            }
        }
        return count;
    }

    private Set<LabelSchemaDescriptor> extractIndexesByProperties( PrimitiveIntCollection properties )
    {
        Set<LabelSchemaDescriptor> set = new HashSet<>();
        for ( PrimitiveIntIterator iterator = properties.iterator(); iterator.hasNext(); )
        {
            Set<LabelSchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                set.addAll( forProperty );
            }
        }
        return set;
    }

    private int countIndexesByProperties( PrimitiveIntCollection properties )
    {
        int count = 0;
        for ( PrimitiveIntIterator iterator = properties.iterator(); iterator.hasNext(); )
        {
            Set<LabelSchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                count += forProperty.size();
            }
        }
        return count;
    }
}
