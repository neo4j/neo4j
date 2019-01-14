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

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.collection.PrimitiveArrays;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.PropertySchemaType.COMPLETE_ALL_TOKENS;
import static org.neo4j.kernel.impl.api.index.EntityUpdates.PropertyValueType.Changed;
import static org.neo4j.kernel.impl.api.index.EntityUpdates.PropertyValueType.NoValue;
import static org.neo4j.kernel.impl.api.index.EntityUpdates.PropertyValueType.UnChanged;

/**
 * Subclasses of this represent events related to property changes due to entity addition, deletion or update.
 * This is of use in populating indexes that might be relevant to label/reltype and property combinations.
 */
public class EntityUpdates implements PropertyLoader.PropertyLoadSink
{
    private final long entityId;
    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    // ASSUMPTION: these long arrays are actually sorted sets
    private long[] entityTokensBefore;
    private long[] entityTokensAfter;

    private final MutableIntObjectMap<PropertyValue> knownProperties;
    private boolean hasLoadedAdditionalProperties;

    public static class Builder
    {
        private EntityUpdates updates;

        private Builder( EntityUpdates updates )
        {
            this.updates = updates;
        }

        public Builder added( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, EntityUpdates.after( value ) );
            return this;
        }

        public Builder removed( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, EntityUpdates.before( value ) );
            return this;
        }

        public Builder changed( int propertyKeyId, Value before, Value after )
        {
            updates.put( propertyKeyId, EntityUpdates.changed( before, after ) );
            return this;
        }

        public Builder existing( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, EntityUpdates.unchanged( value ) );
            return this;
        }

        public Builder withTokens( long... entityTokens )
        {
            this.updates.entityTokensBefore = entityTokens;
            this.updates.entityTokensAfter = entityTokens;
            return this;
        }

        public Builder withTokensAfter( long... entityTokensAfter )
        {
            this.updates.entityTokensAfter = entityTokensAfter;
            return this;
        }

        public EntityUpdates build()
        {
            return updates;
        }
    }

    private void put( int propertyKeyId, PropertyValue propertyValue )
    {
        knownProperties.put( propertyKeyId, propertyValue );
    }

    public static Builder forEntity( long entityId )
    {
        return new Builder( new EntityUpdates( entityId, EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY ) );
    }

    private EntityUpdates( long entityId, long[] entityTokensBefore, long[] entityTokensAfter )
    {
        this.entityId = entityId;
        this.entityTokensBefore = entityTokensBefore;
        this.entityTokensAfter = entityTokensAfter;
        this.knownProperties = new IntObjectHashMap<>();
    }

    public final long getEntityId()
    {
        return entityId;
    }

    long[] entityTokensChanged()
    {
        return PrimitiveArrays.symmetricDifference( entityTokensBefore, entityTokensAfter );
    }

    long[] entityTokensUnchanged()
    {
        return PrimitiveArrays.intersect( entityTokensBefore, entityTokensAfter );
    }

    IntSet propertiesChanged()
    {
        assert !hasLoadedAdditionalProperties : "Calling propertiesChanged() is not valid after non-changed " +
                                                "properties have already been loaded.";
        return knownProperties.keySet().toImmutable();
    }

    @Override
    public void onProperty( int propertyId, Value value )
    {
        knownProperties.put( propertyId, unchanged( value ) );
    }

    /**
     * Matches the provided schema descriptors to the node updates in this object, and generates an IndexEntryUpdate
     * for any index that needs to be updated.
     *
     * Note that unless this object contains a full representation of the node state after the update, the results
     * from this methods will not be correct. In that case, use the propertyLoader variant.
     *
     * @param indexKeys The index keys to generate entry updates for
     * @return IndexEntryUpdates for all relevant index keys
     */
    public <INDEX_KEY extends SchemaDescriptorSupplier> Iterable<IndexEntryUpdate<INDEX_KEY>> forIndexKeys(
            Iterable<INDEX_KEY> indexKeys )
    {
        Iterable<INDEX_KEY> potentiallyRelevant = Iterables.filter( indexKey -> atLeastOneRelevantChange( indexKey.schema() ), indexKeys );

        return gatherUpdatesForPotentials( potentiallyRelevant );
    }

    /**
     * Matches the provided schema descriptors to the entity updates in this object, and generates an IndexEntryUpdate
     * for any index that needs to be updated.
     *
     * In some cases the updates to an entity are not enough to determine whether some index should be affected. For
     * example if we have and index of label :A and property p1, and :A is added to this node, we cannot say whether
     * this should affect the index unless we know if this node has property p1. This get even more complicated for
     * composite indexes. To solve this problem, a propertyLoader is used to load any additional properties needed to
     * make these calls.
     *
     * @param indexKeys The index keys to generate entry updates for
     * @param propertyLoader The property loader used to fetch needed additional properties
     * @param type EntityType of the indexes
     * @return IndexEntryUpdates for all relevant index keys
     */
    public <INDEX_KEY extends SchemaDescriptorSupplier> Iterable<IndexEntryUpdate<INDEX_KEY>> forIndexKeys(
            Iterable<INDEX_KEY> indexKeys, PropertyLoader propertyLoader, EntityType type )
    {
        List<INDEX_KEY> potentiallyRelevant = new ArrayList<>();
        final MutableIntSet additionalPropertiesToLoad = new IntHashSet();

        for ( INDEX_KEY indexKey : indexKeys )
        {
            if ( atLeastOneRelevantChange( indexKey.schema() ) )
            {
                potentiallyRelevant.add( indexKey );
                gatherPropsToLoad( indexKey.schema(), additionalPropertiesToLoad );
            }
        }

        if ( !additionalPropertiesToLoad.isEmpty() )
        {
            loadProperties( propertyLoader, additionalPropertiesToLoad, type );
        }

        return gatherUpdatesForPotentials( potentiallyRelevant );
    }

    @SuppressWarnings( "ConstantConditions" )
    private <INDEX_KEY extends SchemaDescriptorSupplier> Iterable<IndexEntryUpdate<INDEX_KEY>> gatherUpdatesForPotentials(
            Iterable<INDEX_KEY> potentiallyRelevant )
    {
        List<IndexEntryUpdate<INDEX_KEY>> indexUpdates = new ArrayList<>();
        for ( INDEX_KEY indexKey : potentiallyRelevant )
        {
            SchemaDescriptor schema = indexKey.schema();
            boolean relevantBefore = relevantBefore( schema );
            boolean relevantAfter = relevantAfter( schema );
            int[] propertyIds = schema.getPropertyIds();
            if ( relevantBefore && !relevantAfter )
            {
                indexUpdates.add( IndexEntryUpdate.remove( entityId, indexKey, valuesBefore( propertyIds ) ) );
            }
            else if ( !relevantBefore && relevantAfter )
            {
                indexUpdates.add( IndexEntryUpdate.add( entityId, indexKey, valuesAfter( propertyIds ) ) );
            }
            else if ( relevantBefore && relevantAfter )
            {
                if ( valuesChanged( propertyIds, schema.propertySchemaType() ) )
                {
                    indexUpdates.add( IndexEntryUpdate.change( entityId, indexKey, valuesBefore( propertyIds ), valuesAfter( propertyIds ) ) );
                }
            }
        }

        return indexUpdates;
    }

    private boolean relevantBefore( SchemaDescriptor schema )
    {
        return schema.isAffected( entityTokensBefore ) && hasPropsBefore( schema.getPropertyIds(), schema.propertySchemaType() );
    }

    private boolean relevantAfter( SchemaDescriptor schema )
    {
        return schema.isAffected( entityTokensAfter ) && hasPropsAfter( schema.getPropertyIds(), schema.propertySchemaType() );
    }

    private void loadProperties( PropertyLoader propertyLoader, MutableIntSet additionalPropertiesToLoad, EntityType type )
    {
        hasLoadedAdditionalProperties = true;
        propertyLoader.loadProperties( entityId, type, additionalPropertiesToLoad, this );

        // loadProperties removes loaded properties from the input set, so the remaining ones were not on the node
        final IntIterator propertiesWithNoValue = additionalPropertiesToLoad.intIterator();
        while ( propertiesWithNoValue.hasNext() )
        {
            knownProperties.put( propertiesWithNoValue.next(), noValue );
        }
    }

    private void gatherPropsToLoad( SchemaDescriptor schema, MutableIntSet target )
    {
        for ( int propertyId : schema.getPropertyIds() )
        {
            if ( knownProperties.get( propertyId ) == null )
            {
                target.add( propertyId );
            }
        }
    }

    private boolean atLeastOneRelevantChange( SchemaDescriptor schema )
    {
        boolean affectedBefore = schema.isAffected( entityTokensBefore );
        boolean affectedAfter = schema.isAffected( entityTokensAfter );
        if ( affectedBefore && affectedAfter )
        {
            for ( int propertyId : schema.getPropertyIds() )
            {
                if ( knownProperties.containsKey( propertyId ) )
                {
                    return true;
                }
            }
            return false;
        }
        return affectedBefore || affectedAfter;
    }

    private boolean hasPropsBefore( int[] propertyIds, SchemaDescriptor.PropertySchemaType propertySchemaType )
    {
        boolean found = false;
        for ( int propertyId : propertyIds )
        {
            PropertyValue propertyValue = knownProperties.getIfAbsent( propertyId, () -> noValue );
            if ( !propertyValue.hasBefore() )
            {
                if ( propertySchemaType == COMPLETE_ALL_TOKENS )
                {
                    return false;
                }
            }
            else
            {
                found = true;
            }
        }
        return found;
    }

    private boolean hasPropsAfter( int[] propertyIds, SchemaDescriptor.PropertySchemaType propertySchemaType )
    {
        boolean found = false;
        for ( int propertyId : propertyIds )
        {
            PropertyValue propertyValue = knownProperties.getIfAbsent( propertyId, () -> noValue );
            if ( !propertyValue.hasAfter() )
            {
                if ( propertySchemaType == COMPLETE_ALL_TOKENS )
                {
                    return false;
                }
            }
            else
            {
                found = true;
            }
        }
        return found;
    }

    private Value[] valuesBefore( int[] propertyIds )
    {
        Value[] values = new Value[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            values[i] = knownProperties.get( propertyIds[i] ).before;
        }
        return values;
    }

    private Value[] valuesAfter( int[] propertyIds )
    {
        Value[] values = new Value[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            PropertyValue propertyValue = knownProperties.get( propertyIds[i] );
            values[i] = propertyValue == null ? null : propertyValue.after;
        }
        return values;
    }

    /**
     * This method should only be called in a context where you know that your entity is relevant both before and after
     */
    private boolean valuesChanged( int[] propertyIds, SchemaDescriptor.PropertySchemaType propertySchemaType )
    {
        if ( propertySchemaType == COMPLETE_ALL_TOKENS )
        {
            // In the case of indexes were all entries must have all indexed tokens, one of the properties must have changed for us to generate a change.
            for ( int propertyId : propertyIds )
            {
                if ( knownProperties.get( propertyId ).type == Changed )
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            // In the case of indexes were we index incomplete index entries, we need to update as long as _anything_ happened to one of the indexed properties.
            for ( int propertyId : propertyIds )
            {
                PropertyValueType type = knownProperties.get( propertyId ).type;
                if ( type != UnChanged && type != NoValue )
                {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() ).append( "[" ).append( entityId );
        result.append( ", entityTokensBefore:" ).append( Arrays.toString( entityTokensBefore ) );
        result.append( ", entityTokensAfter:" ).append( Arrays.toString( entityTokensAfter ) );
        knownProperties.forEachKeyValue( ( key, propertyValue ) ->
        {
            result.append( ", " );
            result.append( key );
            result.append( " -> " );
            result.append( propertyValue );
        } );
        return result.append( ']' ).toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        EntityUpdates that = (EntityUpdates) o;
        return entityId == that.entityId && Arrays.equals( entityTokensBefore, that.entityTokensBefore ) &&
                Arrays.equals( entityTokensAfter, that.entityTokensAfter );
    }

    @Override
    public int hashCode()
    {

        int result = Objects.hash( entityId );
        result = 31 * result + Arrays.hashCode( entityTokensBefore );
        result = 31 * result + Arrays.hashCode( entityTokensAfter );
        return result;
    }

    enum PropertyValueType
    {
        NoValue,
        Before,
        After,
        UnChanged,
        Changed
    }

    private static class PropertyValue
    {
        private final Value before;
        private final Value after;
        private final PropertyValueType type;

        private PropertyValue( Value before, Value after, PropertyValueType type )
        {
            this.before = before;
            this.after = after;
            this.type = type;
        }

        boolean hasBefore()
        {
            return before != null;
        }

        boolean hasAfter()
        {
            return after != null;
        }

        @Override
        public String toString()
        {
            switch ( type )
            {
            case NoValue:   return "NoValue";
            case Before:    return format( "Before(%s)", before );
            case After:     return format( "After(%s)", after );
            case UnChanged: return format( "UnChanged(%s)", after );
            case Changed:   return format( "Changed(from=%s, to=%s)", before, after );
            default:        throw new IllegalStateException( "This cannot happen!" );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            PropertyValue that = (PropertyValue) o;
            if ( type != that.type )
            {
                return false;
            }

            switch ( type )
            {
            case NoValue:   return true;
            case Before:    return before.equals( that.before );
            case After:     return after.equals( that.after );
            case UnChanged: return after.equals( that.after );
            case Changed:   return before.equals( that.before ) &&
                    after.equals( that.after );
            default:        throw new IllegalStateException( "This cannot happen!" );
            }
        }

        @Override
        public int hashCode()
        {
            int result = before != null ? before.hashCode() : 0;
            result = 31 * result + (after != null ? after.hashCode() : 0);
            result = 31 * result + (type != null ? type.hashCode() : 0);
            return result;
        }
    }

    private static PropertyValue noValue = new PropertyValue( null, null, NoValue );

    private static PropertyValue before( Value value )
    {
        return new PropertyValue( value, null, PropertyValueType.Before );
    }

    private static PropertyValue after( Value value )
    {
        return new PropertyValue( null, value, PropertyValueType.After );
    }

    private static PropertyValue unchanged( Value value )
    {
        return new PropertyValue( value, value, PropertyValueType.UnChanged );
    }

    private static PropertyValue changed( Value before, Value after )
    {
        return new PropertyValue( before, after, PropertyValueType.Changed );
    }
}
