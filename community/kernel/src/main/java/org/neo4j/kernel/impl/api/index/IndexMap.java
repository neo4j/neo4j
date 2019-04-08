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
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
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
    private final MutableLongObjectMap<IndexBackedConstraintDescriptor> uniquenessConstraintsById;
    private final Map<SchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor;
    private final SchemaDescriptorLookupSet<SchemaDescriptor> descriptorsByLabelThenProperty;
    private final SchemaDescriptorLookupSet<SchemaDescriptor> descriptorsByReltypeThenProperty;
    private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> constraintsByLabelThenProperty;
    private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> constraintsByRelTypeThenProperty;

    public IndexMap()
    {
        this( new LongObjectHashMap<>(), new HashMap<>(), new ObjectLongHashMap<>(), new LongObjectHashMap<>() );
    }

    private IndexMap(
            MutableLongObjectMap<IndexProxy> indexesById,
            Map<SchemaDescriptor,IndexProxy> indexesByDescriptor,
            MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor,
            MutableLongObjectMap<IndexBackedConstraintDescriptor> uniquenessConstraintsById )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.uniquenessConstraintsById = uniquenessConstraintsById;
        this.descriptorsByLabelThenProperty = new SchemaDescriptorLookupSet<>();
        this.descriptorsByReltypeThenProperty = new SchemaDescriptorLookupSet<>();
        for ( SchemaDescriptor schema : indexesByDescriptor.keySet() )
        {
            addDescriptorToLookups( schema );
        }
        this.constraintsByLabelThenProperty = new SchemaDescriptorLookupSet<>();
        this.constraintsByRelTypeThenProperty = new SchemaDescriptorLookupSet<>();
        for ( IndexBackedConstraintDescriptor constraint : uniquenessConstraintsById.values() )
        {
            addConstraintToLookups( constraint );
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
        selectIndexesByEntityType( schema.entityType() ).remove( schema );

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

    void putUniquenessConstraint( ConstraintRule rule )
    {
        IndexBackedConstraintDescriptor constraintDescriptor = (IndexBackedConstraintDescriptor) rule.getConstraintDescriptor();
        uniquenessConstraintsById.put( rule.getId(), constraintDescriptor );
        constraintsByLabelThenProperty.add( constraintDescriptor );
    }

    void removeUniquenessConstraint( long constraintId )
    {
        IndexBackedConstraintDescriptor constraint = uniquenessConstraintsById.remove( constraintId );
        if ( constraint != null )
        {
            selectConstraintsByEntityType( constraint.schema().entityType() ).remove( constraint );
        }
    }

    private SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> selectConstraintsByEntityType( EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return constraintsByLabelThenProperty;
        case RELATIONSHIP:
            return constraintsByRelTypeThenProperty;
        default:
            throw new IllegalArgumentException( "Unknown entity type " + entityType );
        }
    }

    private SchemaDescriptorLookupSet<SchemaDescriptor> selectIndexesByEntityType( EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return descriptorsByLabelThenProperty;
        case RELATIONSHIP:
            return descriptorsByReltypeThenProperty;
        default:
            throw new IllegalArgumentException( "Unknown entity type " + entityType );
        }
    }

    boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
    {
        return selectIndexesByEntityType( entityType ).has( labels, propertyKey ) || selectConstraintsByEntityType( entityType ).has( labels, propertyKey );
    }

    boolean hasRelatedSchema( int label, EntityType entityType )
    {
        return selectIndexesByEntityType( entityType ).has( label ) || selectConstraintsByEntityType( entityType ).has( label );
    }

    /**
     * Get all descriptors that would be affected by changes in the input labels and/or properties. The returned
     * descriptors are guaranteed to contain all affected indexes, but might also contain unaffected indexes as
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
        return getRelatedDescriptors( selectIndexesByEntityType( entityType ), changedEntityTokens, unchangedEntityTokens, sortedProperties,
                propertyListIsComplete );
    }

    /**
     * Get all uniqueness constraints that would be affected by changes in the input labels and/or properties. The returned
     * set is guaranteed to contain all affected constraints, but might also contain unaffected constraints as
     * we cannot provide matching without checking unaffected properties for composite indexes.
     *
     * @param changedEntityTokens set of labels that have changed
     * @param unchangedEntityTokens set of labels that are unchanged
     * @param sortedProperties sorted list of properties
     * @param entityType type of indexes to get
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    public Set<IndexBackedConstraintDescriptor> getRelatedConstraints( long[] changedEntityTokens, long[] unchangedEntityTokens, int[] sortedProperties,
            boolean propertyListIsComplete, EntityType entityType )
    {
        return getRelatedDescriptors( selectConstraintsByEntityType( entityType ), changedEntityTokens, unchangedEntityTokens, sortedProperties,
                propertyListIsComplete );
    }

    /**
     * @param changedLabels set of labels that have changed
     * @param unchangedLabels set of labels that are unchanged
     * @param sortedProperties set of properties
     * @param propertyListIsComplete whether or not the property list is complete. For CREATE/DELETE the list is complete, but may not be for UPDATEs.
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    private <T extends SchemaDescriptorSupplier> Set<T> getRelatedDescriptors( SchemaDescriptorLookupSet<T> set, long[] changedLabels, long[] unchangedLabels,
            int[] sortedProperties, boolean propertyListIsComplete )
    {
        if ( set.isEmpty() )
        {
            return Collections.emptySet();
        }

        Set<T> descriptors = new HashSet<>();
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
        return new IndexMap( LongObjectHashMap.newMap( indexesById ), cloneMap( indexesByDescriptor ), new ObjectLongHashMap<>( indexIdsByDescriptor ),
                LongObjectHashMap.newMap( uniquenessConstraintsById ) );
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
        selectIndexesByEntityType( schema.entityType() ).add( schema );
    }

    private void addConstraintToLookups( IndexBackedConstraintDescriptor constraint )
    {
        selectConstraintsByEntityType( constraint.schema().entityType() ).add( constraint );
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
