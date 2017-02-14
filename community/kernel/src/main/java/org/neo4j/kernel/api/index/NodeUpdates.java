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
package org.neo4j.kernel.api.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.index.UpdateMode;
import org.neo4j.kernel.impl.transaction.state.LabelChangeSummary;

import static java.util.Arrays.binarySearch;
import static org.neo4j.kernel.impl.store.ShortArray.EMPTY_LONG_ARRAY;

/**
 * Subclasses of this represent events related to property changes due to node addition, deletion or update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 */
public class NodeUpdates
{
    private final long nodeId;
    private final long[] labelsBefore;
    private final long[] labelsAfter;
    private final LabelChangeSummary labelChangeSummary;

    public static class Builder
    {
        private PropertyUpdate propertyUpdates;
        private NodeUpdates updates;

        private Builder( NodeUpdates updates )
        {
            this.updates = updates;
        }

        private void addUpdate( PropertyUpdate update )
        {
            update.next = propertyUpdates;
            propertyUpdates = update;
        }

        public Builder added( int propertyKeyId, Object value )
        {
            addUpdate( new PropertyAdded( propertyKeyId, value ) );
            return this;
        }

        public Builder removed( int propertyKeyId, Object value )
        {
            addUpdate( new PropertyRemoved( propertyKeyId, value ) );
            return this;
        }

        public Builder changed( int propertyKeyId, Object before, Object after )
        {
            addUpdate( new PropertyChanged( propertyKeyId, before, after ) );
            return this;
        }

        public NodeUpdates build()
        {
            updates.setProperties( propertyUpdates, new DefinedProperty[0] );
            return updates;
        }

        public NodeUpdates buildWithExistingProperties( DefinedProperty... definedProperties )
        {
            updates.setProperties( propertyUpdates, definedProperties );
            return updates;
        }

        public boolean hasUpdatedLabels()
        {
            return updates.labelChangeSummary.hasAddedLabels() || updates.labelChangeSummary.hasRemovedLabels();
        }

        public boolean hasUpdates()
        {
            return propertyUpdates != null || updates.hasIndexingAppropriateUpdates();
        }
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
        this.labelChangeSummary = new LabelChangeSummary( labelsBefore, labelsAfter );
    }

    public final long getNodeId()
    {
        return nodeId;
    }

    private final Map<Integer,Object> propertiesBefore = new HashMap<>();
    private final Map<Integer,Object> propertiesAfter = new HashMap<>();
    private final Map<Integer,Object> propertiesUnchanged = new HashMap<>();

    public Optional<IndexEntryUpdate> forIndex( NewIndexDescriptor index )
    {
        if ( index.schema().getPropertyIds().length > 1 )
        {
            return getMultiPropertyIndexUpdate( index );
        }
        else
        {
            return getSingePropertyIndexUpdate( index );
        }
    }

    private Optional<IndexEntryUpdate> getMultiPropertyIndexUpdate( NewIndexDescriptor index )
    {
        int[] propertyKeyIds = index.schema().getPropertyIds();
        boolean labelExistsBefore = binarySearch( labelsBefore, index.schema().getLabelId() ) >= 0;
        boolean labelExistsAfter = binarySearch( labelsAfter, index.schema().getLabelId() ) >= 0;
        Set<Integer> added = new HashSet<>();   // index specific properties added to this node
        Set<Integer> removed = new HashSet<>(); // index specific properties removed from this node
        Set<Integer> changed = new HashSet<>(); // index specific properties changed on this node
        Set<Integer> unchanged = new HashSet<>(); // index specific properties not changed on this node
        Set<Integer> unknown = new HashSet<>(); // index specific properties not in this node (so cannot index)
        Object[] before = new Object[propertyKeyIds.length];
        Object[] after = new Object[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            int propertyKeyId = propertyKeyIds[i];
            before[i] = propertiesBefore.get( propertyKeyId );
            after[i] = propertiesAfter.get( propertyKeyId );
            Object unchangedValue = propertiesUnchanged.get( propertyKeyId );
            if ( before[i] != null )
            {
                if ( after[i] != null )
                {
                    changed.add( propertyKeyId );
                }
                else
                {
                    removed.add( propertyKeyId );
                }
            }
            else if ( after[i] != null )
            {
                added.add( propertyKeyId );
            }
            else if ( unchangedValue != null )
            {
                before[i] = unchangedValue;
                after[i] = unchangedValue;
                unchanged.add( propertyKeyId );
            }
            else
            {
                unknown.add( propertyKeyId );
                break;
            }
        }
        if ( unknown.size() == 0 )
        {
            if ( added.size() > 0 )
            {
                if ( labelExistsAfter )
                {
                    // Added one or more properties and the label exists
                    return Optional.of( IndexEntryUpdate.add( nodeId, index, after ) );
                }
            }
            else if ( removed.size() > 0 )
            {
                if ( labelExistsBefore )
                {
                    // Removed one or more properties and the label existed
                    return Optional.of( IndexEntryUpdate.remove( nodeId, index, before ) );
                }
            }
            else if ( changed.size() > 0 )
            {
                if ( labelExistsBefore && labelExistsAfter )
                {
                    // Changed one or more properties and the label still exists
                    return Optional.of( IndexEntryUpdate.change( nodeId, index, before, after ) );
                }
                else if ( labelExistsAfter )
                {
                    // Changed one or more properties and added the label
                    return Optional.of( IndexEntryUpdate.add( nodeId, index, after ) );
                }
                else if ( labelExistsBefore )
                {
                    // Changed one or more properties and removed the label
                    return Optional.of( IndexEntryUpdate.remove( nodeId, index, before ) );
                }
            }
            else if ( unchanged.size() >= 0 )
            {
                if ( !labelExistsBefore && labelExistsAfter )
                {
                    // Node has the right properties and the label was added
                    return Optional.of( IndexEntryUpdate.add( nodeId, index, after ) );
                }
                else if ( labelExistsBefore && !labelExistsAfter )
                {
                    // Node has the right property and the label was removed
                    return Optional.of( IndexEntryUpdate.remove( nodeId, index, before ) );
                }
            }
        }
        return Optional.empty();
    }

    private Optional<IndexEntryUpdate> getSingePropertyIndexUpdate( NewIndexDescriptor index )
    {
        int propertyKeyId = index.schema().getPropertyId();
        Object before = propertiesBefore.get( propertyKeyId );
        Object after = propertiesAfter.get( propertyKeyId );
        Object unchanged = propertiesUnchanged.get( propertyKeyId );
        boolean labelExistsBefore = binarySearch( labelsBefore, index.schema().getLabelId() ) >= 0;
        boolean labelExistsAfter = binarySearch( labelsAfter, index.schema().getLabelId() ) >= 0;
        if ( before != null )
        {
            if ( after != null )
            {
                if ( labelExistsBefore && labelExistsAfter )
                {
                    // Changed a property and have the label
                    return Optional.of( IndexEntryUpdate.change( nodeId, index, before, after ) );
                }
                else if ( labelExistsAfter )
                {
                    // Changed a property and added the label
                    return Optional.of( IndexEntryUpdate.add( nodeId, index, after ) );
                }
                else if ( labelExistsBefore )
                {
                    // Changed a property and removed the label
                    return Optional.of( IndexEntryUpdate.remove( nodeId, index, before ) );
                }
            }
            else
            {
                if ( labelExistsBefore )
                {
                    // Removed a property and node had the label
                    return Optional.of( IndexEntryUpdate.remove( nodeId, index, before ) );
                }
            }
        }
        else if ( after != null )
        {
            if ( labelExistsAfter )
            {
                // Added a property and node has the label
                return Optional.of( IndexEntryUpdate.add( nodeId, index, after ) );
            }
        }
        else if ( unchanged != null )
        {
            if ( !labelExistsBefore && labelExistsAfter )
            {
                // Node has the right property and the label was added
                return Optional.of( IndexEntryUpdate.add( nodeId, index, unchanged ) );
            }
            else if ( labelExistsBefore && !labelExistsAfter )
            {
                // Node has the right property and the label was removed
                return Optional.of( IndexEntryUpdate.remove( nodeId, index, unchanged ) );
            }
        }
        return Optional.empty();
    }

    public void setProperties( PropertyUpdate propertyUpdates, DefinedProperty[] definedProperties )
    {
        propertiesBefore.clear();
        propertiesAfter.clear();
        propertiesUnchanged.clear();
        PropertyUpdate current = propertyUpdates;
        while ( current != null )
        {
            switch ( current.mode )
            {
            case ADDED:
                propertiesAfter.put( current.propertyKeyId, current.value );
                break;
            case REMOVED:
                propertiesBefore.put( current.propertyKeyId, current.value );
                break;
            case CHANGED:
                propertiesBefore.put( current.propertyKeyId, ((PropertyChanged) current).before );
                propertiesAfter.put( current.propertyKeyId, current.value );
                break;
            default:
                throw new IllegalStateException( current.mode.toString() );
            }
            current = current.next;
        }
        for ( DefinedProperty property : definedProperties )
        {
            if ( !propertiesAfter.containsKey( property.propertyKeyId() ) &&
                 !propertiesBefore.containsKey( property.propertyKeyId() ) )
            {
                propertiesUnchanged.put( property.propertyKeyId(), property.value() );
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() ).append( "[" ).append( nodeId );
        result.append( ", labelsBefore:" ).append( Arrays.toString( labelsBefore ) );
        result.append( ", labelsAfter:" ).append( Arrays.toString( labelsAfter ) );
        printProperties( result, "Before", propertiesBefore );
        printProperties( result, "After", propertiesAfter );
        printProperties( result, "Unchanged", propertiesUnchanged );
        return result.append( "]" ).toString();
    }

    private void printProperties( StringBuilder result, String suffix, Map<Integer,Object> properties )
    {
        for ( Map.Entry<Integer,Object> entry : properties.entrySet() )
        {
            result.append( ", Property" + suffix + "[" + entry.getKey() + "]: " + entry.getValue() );
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode( labelsBefore );
        result = prime * result + Arrays.hashCode( labelsAfter );
        result = prime * result + (int) (nodeId ^ (nodeId >>> 32));
        result = addPropertiesTohash( prime, result, propertiesBefore );
        result = addPropertiesTohash( prime, result, propertiesAfter );
        return result;
    }

    private int addPropertiesTohash( int prime, int result, Map<Integer,Object> properties )
    {
        for ( int propertyKeyId : properties.keySet() )
        {
            result = prime * result + propertyKeyId;
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
               propertyMapEquals( propertiesBefore, other.propertiesBefore ) &&
               propertyMapEquals( propertiesAfter, other.propertiesAfter ) &&
               propertyMapEquals( propertiesUnchanged, other.propertiesUnchanged );
    }

    private boolean propertyMapEquals( Map<Integer,Object> a, Map<Integer,Object> b )
    {
        if ( a.size() != b.size() )
        {
            return false;
        }
        for ( Map.Entry<Integer,Object> entry : a.entrySet() )
        {
            int key = entry.getKey();
            if ( !b.containsKey( key ) || !propertyValueEqual( b.get( key ), entry.getValue() ) )
            {
                return false;
            }
        }
        return true;
    }

    public boolean hasIndexingAppropriateUpdates()
    {
        boolean propertiesExistOrAdded = !propertiesAfter.isEmpty() || !propertiesUnchanged.isEmpty();
        boolean propertiesExistOrRemoved = !propertiesBefore.isEmpty() || !propertiesUnchanged.isEmpty();
        boolean labelsExistOrAdded = labelChangeSummary.hasAddedLabels() || labelChangeSummary.hasUnchangedLabels();
        boolean labelsExistOrRemoved = labelChangeSummary.hasRemovedLabels() || labelChangeSummary.hasUnchangedLabels();

        boolean hasLabelsAdded = labelChangeSummary.hasAddedLabels() && propertiesExistOrAdded;
        boolean hasLabelsRemoved = labelChangeSummary.hasRemovedLabels() && propertiesExistOrRemoved;
        boolean hasPropertiesAdded = !propertiesAfter.isEmpty() && labelsExistOrAdded;
        boolean hasPropertiesRemoved = !propertiesBefore.isEmpty() && labelsExistOrRemoved;

        return hasLabelsAdded || hasLabelsRemoved || hasPropertiesAdded || hasPropertiesRemoved;
    }

    private static boolean propertyValueEqual( Object a, Object b )
    {
        if ( a == null )
        {
            return b == null;
        }
        if ( b == null )
        {
            return false;
        }

        if ( a instanceof boolean[] && b instanceof boolean[] )
        {
            return Arrays.equals( (boolean[]) a, (boolean[]) b );
        }
        if ( a instanceof byte[] && b instanceof byte[] )
        {
            return Arrays.equals( (byte[]) a, (byte[]) b );
        }
        if ( a instanceof short[] && b instanceof short[] )
        {
            return Arrays.equals( (short[]) a, (short[]) b );
        }
        if ( a instanceof int[] && b instanceof int[] )
        {
            return Arrays.equals( (int[]) a, (int[]) b );
        }
        if ( a instanceof long[] && b instanceof long[] )
        {
            return Arrays.equals( (long[]) a, (long[]) b );
        }
        if ( a instanceof char[] && b instanceof char[] )
        {
            return Arrays.equals( (char[]) a, (char[]) b );
        }
        if ( a instanceof float[] && b instanceof float[] )
        {
            return Arrays.equals( (float[]) a, (float[]) b );
        }
        if ( a instanceof double[] && b instanceof double[] )
        {
            return Arrays.equals( (double[]) a, (double[]) b );
        }
        if ( a instanceof Object[] && b instanceof Object[] )
        {
            return Arrays.equals( (Object[]) a, (Object[]) b );
        }
        return a.equals( b );
    }

    private abstract static class PropertyUpdate
    {
        private final int propertyKeyId;
        private final Object value;
        private final UpdateMode mode;
        private PropertyUpdate next;

        private PropertyUpdate( int propertyKeyId, Object value, UpdateMode mode )
        {
            this.propertyKeyId = propertyKeyId;
            this.value = value;
            this.mode = mode;
        }

        public UpdateMode mode()
        {
            return mode;
        }

        public int propertyKeyId()
        {
            return propertyKeyId;
        }

        public Object value()
        {
            return value;
        }
    }

    private static class PropertyAdded extends PropertyUpdate
    {
        private PropertyAdded( int propertyKeyId, Object value )
        {
            super( propertyKeyId, value, UpdateMode.ADDED );
        }
    }

    private static class PropertyRemoved extends PropertyUpdate
    {
        private PropertyRemoved( int propertyKeyId, Object value )
        {
            super( propertyKeyId, value, UpdateMode.REMOVED );
        }
    }

    public static class PropertyChanged extends PropertyUpdate
    {
        private final Object before;

        private PropertyChanged( int propertyKeyId, Object before, Object after )
        {
            super( propertyKeyId, after, UpdateMode.CHANGED );
            this.before = before;
        }

        public Object before()
        {
            return before;
        }
    }

}
