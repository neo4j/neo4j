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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveArrays;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static java.util.Arrays.binarySearch;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.asSet;
import static org.neo4j.kernel.impl.api.index.NodeUpdates.PropertyValueType.Changed;
import static org.neo4j.kernel.impl.api.index.NodeUpdates.PropertyValueType.NoValue;

/**
 * Subclasses of this represent events related to property changes due to node addition, deletion or update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 */
public class NodeUpdates implements PropertyLoader.PropertyLoadSink
{
    private final long nodeId;
    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    // ASSUMPTION: these long arrays are actually sorted sets
    private final long[] labelsBefore;
    private final long[] labelsAfter;

    private final PrimitiveIntObjectMap<PropertyValue> knownProperties;
    private boolean hasLoadedAdditionalProperties;

    public static class Builder
    {
        private NodeUpdates updates;

        private Builder( NodeUpdates updates )
        {
            this.updates = updates;
        }

        public Builder added( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, NodeUpdates.after( value ) );
            return this;
        }

        public Builder removed( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, NodeUpdates.before( value ) );
            return this;
        }

        public Builder changed( int propertyKeyId, Value before, Value after )
        {
            updates.put( propertyKeyId, NodeUpdates.changed( before, after ) );
            return this;
        }

        public Builder existing( int propertyKeyId, Value value )
        {
            updates.put( propertyKeyId, NodeUpdates.unchanged( value ) );
            return this;
        }

        public NodeUpdates build()
        {
            return updates;
        }
    }

    private void put( int propertyKeyId, PropertyValue propertyValue )
    {
        knownProperties.put( propertyKeyId, propertyValue );
    }

    public static Builder forNode( long nodeId )
    {
        return new Builder( new NodeUpdates( nodeId, EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY ) );
    }

    public static Builder forNode( long nodeId, long[] labels )
    {
        return new Builder( new NodeUpdates( nodeId, labels, labels ) );
    }

    public static Builder forNode( long nodeId, long[] labelsBefore, long[] labelsAfter )
    {
        return new Builder( new NodeUpdates( nodeId, labelsBefore, labelsAfter ) );
    }

    private NodeUpdates( long nodeId, long[] labelsBefore, long[] labelsAfter )
    {
        this.nodeId = nodeId;
        this.labelsBefore = labelsBefore;
        this.labelsAfter = labelsAfter;
        this.knownProperties = Primitive.intObjectMap();
    }

    public final long getNodeId()
    {
        return nodeId;
    }

    long[] labelsChanged()
    {
        return PrimitiveArrays.symmetricDifference( labelsBefore, labelsAfter );
    }

    long[] labelsUnchanged()
    {
        return PrimitiveArrays.intersect( labelsBefore, labelsAfter );
    }

    PrimitiveIntSet propertiesChanged()
    {
        assert !hasLoadedAdditionalProperties : "Calling propertiesChanged() is not valid after non-changed " +
                                                "properties have already been loaded.";
        return asSet( knownProperties.iterator() );
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
        Iterable<INDEX_KEY> potentiallyRelevant = Iterables.filter( this::atLeastOneRelevantChange, indexKeys );

        return gatherUpdatesForPotentials( potentiallyRelevant );
    }

    /**
     * Matches the provided schema descriptors to the node updates in this object, and generates an IndexEntryUpdate
     * for any index that needs to be updated.
     *
     * In some cases the updates to a node are not enough to determine whether some index should be affected. For
     * example if we have and index of label :A and property p1, and :A is added to this node, we cannot say whether
     * this should affect the index unless we know if this node has property p1. This get even more complicated for
     * composite indexes. To solve this problem, a propertyLoader is used to load any additional properties needed to
     * make these calls.
     *
     * @param indexKeys The index keys to generate entry updates for
     * @param propertyLoader The property loader used to fetch needed additional properties
     * @return IndexEntryUpdates for all relevant index keys
     */
    public <INDEX_KEY extends SchemaDescriptorSupplier> Iterable<IndexEntryUpdate<INDEX_KEY>> forIndexKeys(
            Iterable<INDEX_KEY> indexKeys, PropertyLoader propertyLoader )
    {
        List<INDEX_KEY> potentiallyRelevant = new ArrayList<>();
        PrimitiveIntSet additionalPropertiesToLoad = Primitive.intSet();

        for ( INDEX_KEY indexKey : indexKeys )
        {
            if ( atLeastOneRelevantChange( indexKey ) )
            {
                potentiallyRelevant.add( indexKey );
                gatherPropsToLoad( indexKey.schema(), additionalPropertiesToLoad );
            }
        }

        if ( !additionalPropertiesToLoad.isEmpty() )
        {
            loadProperties( propertyLoader, additionalPropertiesToLoad );
        }

        return gatherUpdatesForPotentials( potentiallyRelevant );
    }

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
                indexUpdates.add( IndexEntryUpdate.remove(
                        nodeId, indexKey, valuesBefore( propertyIds )
                    ) );
            }
            else if ( !relevantBefore && relevantAfter )
            {
                indexUpdates.add( IndexEntryUpdate.add(
                        nodeId, indexKey, valuesAfter( propertyIds )
                ) );
            }
            else if ( relevantBefore )
            {
                if ( valuesChanged( propertyIds ) )
                {
                    indexUpdates.add( IndexEntryUpdate.change(
                            nodeId, indexKey, valuesBefore( propertyIds ), valuesAfter( propertyIds ) ) );
                }
            }
        }

        return indexUpdates;
    }

    private boolean relevantBefore( SchemaDescriptor schema )
    {
        return hasLabel( schema.keyId(), labelsBefore ) &&
                hasPropsBefore( schema.getPropertyIds() );
    }

    private boolean relevantAfter( SchemaDescriptor schema )
    {
        return hasLabel( schema.keyId(), labelsAfter ) &&
                hasPropsAfter( schema.getPropertyIds() );
    }

    private void loadProperties( PropertyLoader propertyLoader, PrimitiveIntSet additionalPropertiesToLoad )
    {
        hasLoadedAdditionalProperties = true;
        propertyLoader.loadProperties( nodeId, additionalPropertiesToLoad, this );

        // loadProperties removes loaded properties from the input set, so the remaining ones were not on the node
        PrimitiveIntIterator propertiesWithNoValue = additionalPropertiesToLoad.iterator();
        while ( propertiesWithNoValue.hasNext() )
        {
            knownProperties.put( propertiesWithNoValue.next(), noValue );
        }
    }

    private void gatherPropsToLoad( SchemaDescriptor schema, PrimitiveIntSet target )
    {
        for ( int propertyId : schema.getPropertyIds() )
        {
            if ( knownProperties.get( propertyId ) == null )
            {
                target.add( propertyId );
            }
        }
    }

    private boolean atLeastOneRelevantChange( SchemaDescriptorSupplier indexKey )
    {
        int labelId = indexKey.schema().keyId();
        boolean labelBefore = hasLabel( labelId, labelsBefore );
        boolean labelAfter = hasLabel( labelId, labelsAfter );
        if ( labelBefore && labelAfter )
        {
            for ( int propertyId : indexKey.schema().getPropertyIds() )
            {
                if ( knownProperties.get( propertyId ) != null )
                {
                    return true;
                }
            }
            return false;
        }
        return labelBefore || labelAfter;
    }

    private boolean hasLabel( int labelId, long[] labels )
    {
        return binarySearch( labels, labelId ) >= 0;
    }

    private boolean hasPropsBefore( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            PropertyValue propertyValue = knownProperties.get( propertyId );
            if ( propertyValue == null || !propertyValue.hasBefore() )
            {
                return false;
            }
        }
        return true;
    }

    private boolean hasPropsAfter( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            PropertyValue propertyValue = knownProperties.get( propertyId );
            if ( propertyValue == null || !propertyValue.hasAfter() )
            {
                return false;
            }
        }
        return true;
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

    private boolean valuesChanged( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            if ( knownProperties.get( propertyId ).type == Changed )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() ).append( "[" ).append( nodeId );
        result.append( ", labelsBefore:" ).append( Arrays.toString( labelsBefore ) );
        result.append( ", labelsAfter:" ).append( Arrays.toString( labelsAfter ) );
        knownProperties.visitEntries( ( key, propertyValue ) ->
        {
            result.append( ", " );
            result.append( key );
            result.append( " -> " );
            result.append( propertyValue );
            return false;
        } );
        return result.append( ']' ).toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode( labelsBefore );
        result = prime * result + Arrays.hashCode( labelsAfter );
        result = prime * result + (int) (nodeId ^ (nodeId >>> 32));

        PrimitiveIntIterator propertyKeyIds = knownProperties.iterator();
        while ( propertyKeyIds.hasNext() )
        {
            int propertyKeyId = propertyKeyIds.next();
            result = result * 31 + propertyKeyId;
            result = result * 31 + knownProperties.get( propertyKeyId ).hashCode();
        }
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        NodeUpdates other = (NodeUpdates) obj;
        return Arrays.equals( labelsBefore, other.labelsBefore ) &&
               Arrays.equals( labelsAfter, other.labelsAfter ) &&
               nodeId == other.nodeId &&
               propertyMapEquals( knownProperties, other.knownProperties );
    }

    private boolean propertyMapEquals(
            PrimitiveIntObjectMap<PropertyValue> a,
            PrimitiveIntObjectMap<PropertyValue> b )
    {
        if ( a.size() != b.size() )
        {
            return false;
        }
        PrimitiveIntIterator aIterator = a.iterator();
        while ( aIterator.hasNext() )
        {
            int key = aIterator.next();
            if ( !a.get( key ).equals( b.get( key ) ) )
            {
                return false;
            }
        }
        return true;
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
