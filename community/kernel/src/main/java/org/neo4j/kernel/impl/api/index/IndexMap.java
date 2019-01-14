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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final PrimitiveLongObjectMap<IndexProxy> indexesById;
    private final Map<SchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final Map<SchemaDescriptor,Long> indexIdsByDescriptor;
    private final PrimitiveIntObjectMap<Set<SchemaDescriptor>> descriptorsByLabel;
    private final PrimitiveIntObjectMap<Set<SchemaDescriptor>> descriptorsByProperty;

    public IndexMap()
    {
        this( Primitive.longObjectMap(), new HashMap<>(), new HashMap<>() );
    }

    IndexMap( PrimitiveLongObjectMap<IndexProxy> indexesById )
    {
        this( indexesById, indexesByDescriptor( indexesById ), indexIdsByDescriptor( indexesById ) );
    }

    private IndexMap(
            PrimitiveLongObjectMap<IndexProxy> indexesById,
            Map<SchemaDescriptor,IndexProxy> indexesByDescriptor,
            Map<SchemaDescriptor,Long> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.descriptorsByLabel = Primitive.intObjectMap();
        this.descriptorsByProperty = Primitive.intObjectMap();
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

    public void putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        SchemaDescriptor schema = indexProxy.getDescriptor().schema();
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

        SchemaDescriptor schema = removedProxy.getDescriptor().schema();
        indexesByDescriptor.remove( schema );

        removeFromLookup( schema.keyId(), schema, descriptorsByLabel );
        for ( int propertyId : schema.getPropertyIds() )
        {
            removeFromLookup( propertyId, schema, descriptorsByProperty );
        }

        return removedProxy;
    }

    public void forEachIndexProxy( BiConsumer<Long, IndexProxy> consumer )
    {
        indexesById.visitEntries( ( key, indexProxy ) ->
        {
            consumer.accept( key, indexProxy);
            return false;
        } );
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
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    public Set<SchemaDescriptor> getRelatedIndexes(
            long[] changedLabels, long[] unchangedLabels, PrimitiveIntSet properties )
    {
        if ( changedLabels.length == 1 && properties.isEmpty() )
        {
            Set<SchemaDescriptor> descriptors = descriptorsByLabel.get( (int)changedLabels[0] );
            return descriptors == null ? Collections.emptySet() : descriptors;
        }

        if ( changedLabels.length == 0 && properties.size() == 1 )
        {
            return getDescriptorsByProperties( unchangedLabels, properties );
        }

        Set<SchemaDescriptor> descriptors = extractIndexesByLabels( changedLabels );
        descriptors.addAll( getDescriptorsByProperties( unchangedLabels, properties ) );

        return descriptors;
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( clonePrimitiveMap( indexesById ), cloneMap( indexesByDescriptor ),
                cloneMap( indexIdsByDescriptor ) );
    }

    public Iterator<SchemaDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public PrimitiveLongIterator indexIds()
    {
        return indexesById.iterator();
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

    private PrimitiveLongObjectMap<IndexProxy> clonePrimitiveMap( PrimitiveLongObjectMap<IndexProxy> indexesById )
    {
        return PrimitiveLongCollections.copy( indexesById );
    }

    private void addDescriptorToLookups( SchemaDescriptor schema )
    {
        addToLookup( schema.keyId(), schema, descriptorsByLabel );

        for ( int propertyId : schema.getPropertyIds() )
        {
            addToLookup( propertyId, schema, descriptorsByProperty );
        }
    }

    private void addToLookup(
            int key,
            SchemaDescriptor schema,
            PrimitiveIntObjectMap<Set<SchemaDescriptor>> lookup )
    {
        Set<SchemaDescriptor> descriptors = lookup.get( key );
        if ( descriptors == null )
        {
            descriptors = new HashSet<>();
            lookup.put( key, descriptors );
        }
        descriptors.add( schema );
    }

    private void removeFromLookup(
            int key,
            SchemaDescriptor schema,
            PrimitiveIntObjectMap<Set<SchemaDescriptor>> lookup )
    {
        Set<SchemaDescriptor> descriptors = lookup.get( key );
        descriptors.remove( schema );
        if ( descriptors.isEmpty() )
        {
            lookup.remove( key );
        }
    }

    private static Map<SchemaDescriptor, IndexProxy> indexesByDescriptor( PrimitiveLongObjectMap<IndexProxy> indexesById )
    {
        Map<SchemaDescriptor, IndexProxy> map = new HashMap<>();
        for ( IndexProxy proxy : indexesById.values() )
        {
            map.put( proxy.schema(), proxy );
        }
        return map;
    }

    private static Map<SchemaDescriptor, Long> indexIdsByDescriptor( PrimitiveLongObjectMap<IndexProxy> indexesById )
    {
        Map<SchemaDescriptor, Long> map = new HashMap<>();
        indexesById.visitEntries( ( key, indexProxy ) ->
        {
            map.put( indexProxy.schema(), key );
            return false;
        } );
        return map;
    }

    /**
     * Get descriptors affected by changed properties. Implementation checks whether doing
     * the lookup using the unchanged labels or the changed properties given the smallest final
     * set of indexes, and chooses the best way.
     *
     * @param unchangedLabels set of labels that are unchanged
     * @param properties set of properties that have changed
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    private Set<SchemaDescriptor> getDescriptorsByProperties(
            long[] unchangedLabels,
            PrimitiveIntSet properties )
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

    private Set<SchemaDescriptor> extractIndexesByLabels( long[] labels )
    {
        Set<SchemaDescriptor> set = new HashSet<>();
        for ( long label : labels )
        {
            Set<SchemaDescriptor> forLabel = descriptorsByLabel.get( (int) label );
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
            Set<SchemaDescriptor> forLabel = descriptorsByLabel.get( (int) label );
            if ( forLabel != null )
            {
                count += forLabel.size();
            }
        }
        return count;
    }

    private Set<SchemaDescriptor> extractIndexesByProperties( PrimitiveIntSet properties )
    {
        Set<SchemaDescriptor> set = new HashSet<>();
        for ( PrimitiveIntIterator iterator = properties.iterator(); iterator.hasNext(); )
        {
            Set<SchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                set.addAll( forProperty );
            }
        }
        return set;
    }

    private int countIndexesByProperties( PrimitiveIntSet properties )
    {
        int count = 0;
        for ( PrimitiveIntIterator iterator = properties.iterator(); iterator.hasNext(); )
        {
            Set<SchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                count += forProperty.size();
            }
        }
        return count;
    }
}
