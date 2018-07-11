/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.ANY;
import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.CONSISTENT_NUMBER_VALUE;
import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.VALUE;
import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.VALUE_GROUP;

public class SchemaCalculator
{
    private Map<SortedLabels,Set<Integer>> labelSetToPropertyKeysMapping;
    private Map<Pair<SortedLabels,Integer>,ValueTypeDecider> labelSetANDNodePropertyKeyIdToValueTypeMapping;
    private Map<Integer,String> labelIdToLabelNameMapping;
    private Map<Integer,String> propertyIdToPropertylNameMapping;
    private Map<Integer,String> relationshipTypIdToRelationshipNameMapping;
    private Map<Integer,Set<Integer>> relationshipTypeIdToPropertyKeysMapping;
    private Map<Pair<Integer,Integer>,ValueTypeDecider> relationshipTypeIdANDPropertyTypeIdToValueTypeMapping;

    private final Set<Integer> emptyPropertyIdSet = Collections.unmodifiableSet( Collections.emptySet() );
    private static final String NODE = "Node";
    private static final String RELATIONSHIP = "Relationship";
    private static final String NULLABLE = "?";

    private final Read dataRead;
    private final TokenRead tokenRead;
    private final CursorFactory cursors;

    SchemaCalculator( Transaction ktx )
    {
        dataRead = ktx.dataRead();
        tokenRead = ktx.tokenRead();
        cursors = ktx.cursors();

        // setup mappings
        int labelCount = tokenRead.labelCount();
        int relationshipTypeCount = tokenRead.relationshipTypeCount();
        labelSetToPropertyKeysMapping = new HashMap<>( labelCount );
        labelIdToLabelNameMapping = new HashMap<>( labelCount );
        propertyIdToPropertylNameMapping = new HashMap<>( tokenRead.propertyKeyCount() );
        relationshipTypIdToRelationshipNameMapping = new HashMap<>( relationshipTypeCount );
        relationshipTypeIdToPropertyKeysMapping = new HashMap<>( relationshipTypeCount );
        labelSetANDNodePropertyKeyIdToValueTypeMapping = new HashMap<>();
        relationshipTypeIdANDPropertyTypeIdToValueTypeMapping = new HashMap<>();
    }

    public Stream<SchemaInfoResult> calculateTabularResultStream()
    {
        calculateSchema();

        List<SchemaInfoResult> results = new ArrayList<>();
        results.addAll( produceResultsForNodes() );
        results.addAll( produceResultsForRelationships() );

        return results.stream();
    }

    private List<SchemaInfoResult> produceResultsForRelationships()
    {
        List<SchemaInfoResult> results = new ArrayList<>();
        for ( Integer typeId : relationshipTypeIdToPropertyKeysMapping.keySet() )
        {
            // lookup typ name
            String name = relationshipTypIdToRelationshipNameMapping.get( typeId );

            // lookup property value types
            Set<Integer> propertyIds = relationshipTypeIdToPropertyKeysMapping.get( typeId );
            if ( propertyIds.size() == 0 )
            {
                results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), null, null ) );
            }
            else
            {
                propertyIds.forEach( propId -> {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeDecider valueTypeDecider = relationshipTypeIdANDPropertyTypeIdToValueTypeMapping.get( Pair.of( typeId, propId ) );
                    results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), propName, valueTypeDecider.getCypherTypeString() ) );
                } );
            }
        }
        return results;
    }

    private List<SchemaInfoResult> produceResultsForNodes()
    {
        List<SchemaInfoResult> results = new ArrayList<>();
        for ( SortedLabels labelSet : labelSetToPropertyKeysMapping.keySet() )
        {
            // lookup label names and produce list of names
            List<String> labelNames = new ArrayList<>();
            for ( int i = 0; i < labelSet.numberOfLabels(); i++ )
            {
                String name = labelIdToLabelNameMapping.get( labelSet.label( i ) );
                labelNames.add( name );
            }

            // lookup property value types
            Set<Integer> propertyIds = labelSetToPropertyKeysMapping.get( labelSet );
            if ( propertyIds.size() == 0 )
            {
                results.add( new SchemaInfoResult( NODE, labelNames, null, null ) );
            }
            else
            {
                propertyIds.forEach( propId -> {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeDecider valueTypeDecider = labelSetANDNodePropertyKeyIdToValueTypeMapping.get( Pair.of( labelSet, propId ) );
                    results.add( new SchemaInfoResult( NODE, labelNames, propName, valueTypeDecider.getCypherTypeString() ) );
                } );
            }
        }
        return results;
    }

    //TODO: If we would have this schema information in the count store (or somewhere), this could be super fast
    private void calculateSchema()
    {
        scanEverythingBelongingToNodes();
        scanEverythingBelongingToRelationships();

        // OTHER:
        // go through all labels
        addNamesToCollection( tokenRead.labelsGetAllTokens(), labelIdToLabelNameMapping );
        // go through all propertyKeys
        addNamesToCollection( tokenRead.propertyKeyGetAllTokens(), propertyIdToPropertylNameMapping );
        // go through all relationshipTypes
        addNamesToCollection( tokenRead.relationshipTypesGetAllTokens(), relationshipTypIdToRelationshipNameMapping );
    }

    private void scanEverythingBelongingToRelationships()
    {
        try ( RelationshipScanCursor relationshipScanCursor = cursors.allocateRelationshipScanCursor();
                PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            dataRead.allRelationshipsScan( relationshipScanCursor );
            while ( relationshipScanCursor.next() )
            {
                int typeId = relationshipScanCursor.type();
                relationshipScanCursor.properties( propertyCursor );
                Set<Integer> propertyIds = new HashSet<>();

                while ( propertyCursor.next() )
                {
                    int propertyKey = propertyCursor.propertyKey();

                    Value currentValue = propertyCursor.propertyValue();
                    Pair<Integer,Integer> key = Pair.of( typeId, propertyKey );
                    updateValueTypeInMapping( currentValue, key, relationshipTypeIdANDPropertyTypeIdToValueTypeMapping );

                    propertyIds.add( propertyKey );
                }
                propertyCursor.close();

                Set<Integer> oldPropertyKeySet = relationshipTypeIdToPropertyKeysMapping.getOrDefault( typeId, emptyPropertyIdSet );

                // find out which old properties we did not visited and mark them as nullable
                if ( !(oldPropertyKeySet == emptyPropertyIdSet) )
                {
                    // we can and need to skip this if we found the empty set
                    oldPropertyKeySet.removeAll( propertyIds );
                    oldPropertyKeySet.forEach( id -> {
                        Pair<Integer,Integer> key = Pair.of( typeId, id );
                        relationshipTypeIdANDPropertyTypeIdToValueTypeMapping.get( key ).setNullable();
                    } );
                }

                propertyIds.addAll( oldPropertyKeySet );
                relationshipTypeIdToPropertyKeysMapping.put( typeId, propertyIds );
            }
            relationshipScanCursor.close();
        }
    }

    private void scanEverythingBelongingToNodes()
    {
        try ( NodeCursor nodeCursor = cursors.allocateNodeCursor();
                PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            dataRead.allNodesScan( nodeCursor );
            while ( nodeCursor.next() )
            {
                // each node
                SortedLabels labels = SortedLabels.from( nodeCursor.labels() );
                nodeCursor.properties( propertyCursor );
                Set<Integer> propertyIds = new HashSet<>();

                while ( propertyCursor.next() )
                {
                    Value currentValue = propertyCursor.propertyValue();
                    int propertyKeyId = propertyCursor.propertyKey();
                    Pair<SortedLabels,Integer> key = Pair.of( labels, propertyKeyId );
                    updateValueTypeInMapping( currentValue, key, labelSetANDNodePropertyKeyIdToValueTypeMapping );

                    propertyIds.add( propertyKeyId );
                }
                propertyCursor.close();

                Set<Integer> oldPropertyKeySet = labelSetToPropertyKeysMapping.getOrDefault( labels, emptyPropertyIdSet );

                // find out which old properties we did not visited and mark them as nullable
                if ( !(oldPropertyKeySet == emptyPropertyIdSet) )
                {
                    // we can and need (!) to skip this if we found the empty set
                    oldPropertyKeySet.removeAll( propertyIds );
                    oldPropertyKeySet.forEach( id -> {
                        Pair<SortedLabels,Integer> key = Pair.of( labels, id );
                        labelSetANDNodePropertyKeyIdToValueTypeMapping.get( key ).setNullable();
                    } );
                }

                propertyIds.addAll( oldPropertyKeySet );
                labelSetToPropertyKeysMapping.put( labels, propertyIds );
            }
            nodeCursor.close();
        }
    }

    private <X, Y> void updateValueTypeInMapping( Value currentValue, Pair<X,Y> key, Map<Pair<X,Y>,ValueTypeDecider> mappingToUpdate )
    {
        ValueTypeDecider decider = mappingToUpdate.get( key );
        if ( decider == null )
        {
            decider = new ValueTypeDecider( currentValue );
            mappingToUpdate.put( key, decider );
        }
        else
        {
            decider.compareAndPutValueType( currentValue );
        }
    }

    private void addNamesToCollection( Iterator<NamedToken> labelIterator, Map<Integer,String> collection )
    {
        while ( labelIterator.hasNext() )
        {
            NamedToken label = labelIterator.next();
            collection.put( label.id(), label.name() );
        }
    }

    private class ValueTypeDecider
    {
        private Value concreteValue;
        private ValueGroup valueGroup;
        private ValueStatus valueStatus;
        private boolean isNullable;
        private Boolean isIntegral;  // this is only important if we have a NumberValue or NumberArray. In those cases false means FloatingPoint

        ValueTypeDecider( Value v )
        {
            if ( v == null )
            {
                throw new IllegalArgumentException();
            }
            this.concreteValue = v;
            this.valueGroup = v.valueGroup();
            this.valueStatus = VALUE;

            this.isIntegral = isIntegral( v );
        }

        private void setNullable( )
        {
            isNullable = true;
        }

        /***
         * Checks if the given value is an Integral value, a Floating Point value or none
         * @param v the given value
         * @return true, if v is an IntegralValue or -Array (e.g. Long), false if v is a FloatingPoint or -Array (e.g. Double)
         * or null if v is not neither NUMBER nor NUMBER_ARRAY
         */

        private Boolean isIntegral( Value v )
        {
            if ( v.valueGroup() == ValueGroup.NUMBER_ARRAY )
            {
                return v instanceof IntegralArray;
            }
            else if ( v.valueGroup() == ValueGroup.NUMBER )
            {
                return v instanceof IntegralValue;
            }
            return null;
        }

        /*
        This method translates an ValueTypeDecider into the correct String
        */
        String getCypherTypeString()
        {
            switch ( valueStatus )
            {
            case VALUE:
                return isNullable ? concreteValue.getTypeName().toUpperCase() + NULLABLE
                                  : concreteValue.getTypeName().toUpperCase();
            case CONSISTENT_NUMBER_VALUE:
                if ( isIntegral == null )
                {
                    throw new IllegalStateException( "isIntegral should have been set in this state" );
                }
                if ( valueGroup == ValueGroup.NUMBER )
                {
                    if ( isIntegral )
                    {
                        return isNullable ? ValueName.NULLABLE_INTEGRAL.asString()
                                          : ValueName.INTEGRAL.asString();
                    }
                    else
                    {
                        return isNullable ? ValueName.NULLABLE_FLOATING_POINT.asString()
                                          : ValueName.FLOATING_POINT.asString();
                    }
                }
                // NUMBER_ARRAY
                if ( isIntegral )
                {
                    return isNullable ? ValueName.NULLABLE_INTEGRAL_ARRAY.asString()
                                      : ValueName.INTEGRAL_ARRAY.asString();
                }
                else
                {
                    return isNullable ? ValueName.NULLABLE_FLOATING_POINT_ARRAY.asString()
                                      : ValueName.FLOATING_POINT_ARRAY.asString();
                }

            case VALUE_GROUP:
                return isNullable ? valueGroup.name() + NULLABLE
                                  : valueGroup.name();
            case ANY:
                return isNullable ? ValueName.NULLABLE_ANYVALUE.asString()
                                  : ValueName.ANYVALUE.asString();
            default:
                throw new IllegalStateException( "Did not recognize ValueStatus" );
            }
        }

        /*
        This method is needed to handle conflicting property values and sets valueStatus accordingly to:
         A) VALUE if current and new value match on class level
         B) CONSISTENT_NUMBER_VALUE if current and new value are NUMBER or NUMBER_ARRAY and both are integral or both are floating point values
         C) VALUE_GROUP if at least the ValueGroups of the current and new values match
         D) ANY if nothing matches
        */
        void compareAndPutValueType( Value newValue )
        {
            if ( newValue == null )
            {
                throw new IllegalArgumentException();
            }

            switch ( valueStatus )
            {
            case VALUE:
                // check if classes match -> if so, do nothing
                if ( !concreteValue.getClass().equals( newValue.getClass() ) )
                {
                    // Clases don't match -> update needed
                    if ( valueGroup.equals( newValue.valueGroup() ) )
                    {
                        // same valueGroup -> set that (default, can be overriden if they are Numbers and consistency checks out)
                        valueStatus = VALUE_GROUP;

                        if ( valueGroup == ValueGroup.NUMBER_ARRAY || valueGroup == ValueGroup.NUMBER )
                        {
                            Boolean newValueIsIntegral = isIntegral( newValue );
                            if ( isIntegral == null || newValueIsIntegral == null )
                            {
                                throw new IllegalStateException(
                                        "isIntegral should have been set in this state and method should have returned non null for new value" );
                            }
                            // test consistency
                            if ( isIntegral == newValueIsIntegral )
                            {
                                valueStatus = CONSISTENT_NUMBER_VALUE;
                            }
                        }
                    }
                    else
                    {
                        // Not same valueGroup -> update to AnyValue
                        valueStatus = ANY;
                    }
                }
                break;

            case CONSISTENT_NUMBER_VALUE:
                if ( !valueGroup.equals( newValue.valueGroup() ) )
                {
                    // not same valueGroup -> update to AnyValue
                    valueStatus = ANY;
                }
                else
                {
                    // same value-group
                    // -> update to VALUE_GROUP if new value brakes consistency
                    Boolean newValueIsIntegral = isIntegral( newValue );
                    if ( isIntegral == null || newValueIsIntegral == null )
                    {
                        throw new IllegalStateException(
                                "isIntegral should have been set in this state and method should have returned non null for new value" );
                    }
                    if ( ! (isIntegral == newValueIsIntegral) )
                    {
                        valueStatus = VALUE_GROUP;
                    }
                }
                break;
            case VALUE_GROUP:
                if ( !valueGroup.equals( newValue.valueGroup() ) )
                {
                    // not same valueGroup -> update to AnyValue
                    valueStatus = ANY;
                }
                break;
            case ANY:
                // DO nothing, cannot go higher
                break;
            default:
                throw new IllegalStateException( "Did not recognize ValueStatus" );
            }
        }
    }

    enum ValueStatus
    {
        VALUE,
        CONSISTENT_NUMBER_VALUE,
        VALUE_GROUP,
        ANY
    }

    enum ValueName
    {
        ANYVALUE( "ANY" ),
        INTEGRAL( "INTEGRAL" ),
        INTEGRAL_ARRAY( "INTEGRALARRAY" ),
        FLOATING_POINT( "FLOATINGPOINT" ),
        FLOATING_POINT_ARRAY( "FLOATINGPOINTARRAY" ),
        NULLABLE_ANYVALUE( ANYVALUE.asString() + NULLABLE ),
        NULLABLE_INTEGRAL( INTEGRAL.asString() + NULLABLE ),
        NULLABLE_INTEGRAL_ARRAY( INTEGRAL_ARRAY.asString() + NULLABLE ),
        NULLABLE_FLOATING_POINT_ARRAY( FLOATING_POINT_ARRAY.asString() + NULLABLE ),
        NULLABLE_FLOATING_POINT( FLOATING_POINT.asString() + NULLABLE );

        private final String textRepresentation;

        ValueName( String textRepresentation )
        {
            this.textRepresentation = textRepresentation;
        }

        String asString()
        {
            return textRepresentation;
        }
    }
}
