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
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;

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

    public IndexMap(
            Map<Long, IndexProxy> indexesById )
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
     * Get all indexes that would be affected by changes in the input labels and/or properties
     * @param labels set of labels
     * @param properties set of properties
     * @return set of LabelSchemaDescriptors describing the effected indexes
     */
    public Set<LabelSchemaDescriptor> getRelatedIndexes( long[] labels, PrimitiveIntCollection properties )
    {
        if ( labels.length == 1 && properties.isEmpty() )
        {
            Set<LabelSchemaDescriptor> descriptors = descriptorsByLabel.get( (int)labels[0] );
            return descriptors == null ? Collections.emptySet() : descriptors;
        }

        if ( labels.length == 0 && properties.size() == 1 )
        {
            Set<LabelSchemaDescriptor> descriptors = descriptorsByProperty.get( properties.iterator().next() );
            return descriptors == null ? Collections.emptySet() : descriptors;
        }

        Set<LabelSchemaDescriptor> descriptors = new HashSet<>();
        for ( long label : labels )
        {
            Set<LabelSchemaDescriptor> toAdd = descriptorsByLabel.get( (int) label );
            if ( toAdd != null )
            {
                descriptors.addAll( toAdd );
            }
        }
        for ( PrimitiveIntIterator property = properties.iterator(); property.hasNext(); )
        {
            Set<LabelSchemaDescriptor> toAdd = descriptorsByProperty.get( property.next() );
            if ( toAdd != null )
            {
                descriptors.addAll( toAdd );
            }
        }
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

    public static Map<LabelSchemaDescriptor, IndexProxy> indexesByDescriptor( Map<Long, IndexProxy> indexesById )
    {
        Map<LabelSchemaDescriptor, IndexProxy> map = new HashMap<>();
        for ( IndexProxy proxy : indexesById.values() )
        {
            map.put( proxy.schema(), proxy );
        }
        return map;
    }

    public static Map<LabelSchemaDescriptor, Long> indexIdsByDescriptor( Map<Long, IndexProxy> indexesById )
    {
        Map<LabelSchemaDescriptor, Long> map = new HashMap<>();
        for ( Map.Entry<Long,IndexProxy> entry : indexesById.entrySet() )
        {
            map.put( entry.getValue().schema(), entry.getKey() );
        }
        return map;
    }
}
