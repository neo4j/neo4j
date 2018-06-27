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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.ANY;
import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.VALUE;
import static org.neo4j.kernel.builtinprocs.SchemaCalculator.ValueStatus.VALUE_GROUP;

public class SchemaCalculator
{
    private KernelTransaction ktx;

    private Map<LabelSet,Set<Integer>> labelSetToPropertyKeysMapping;
    private Map<Pair<LabelSet,Integer>,ValueTypeDecider> labelSetANDNodePropertyKeyIdToValueTypeMapping;
    private Map<Integer,String> labelIdToLabelNameMapping;
    private Map<Integer,String> propertyIdToPropertylNameMapping;
    private Map<Integer,String> relationshipTypIdToRelationshipNameMapping;
    private Map<Integer,Set<Integer>> relationshipTypeIdToPropertyKeysMapping;
    private Map<Pair<Integer,Integer>,ValueTypeDecider> relationshipTypeIdANDPropertyTypeIdToValueTypeMapping;

    private final Set<Integer> emptyPropertyIdSet = Collections.unmodifiableSet( Collections.emptySet() );
    private final String ANYVALUE = "ANY";
    private final String NULLABLE = "?";
    private final String NULLABLE_ANYVALUE = ANYVALUE + NULLABLE;
    private final String NODE = "Node";
    private final String RELATIONSHIP = "Relationship";

    //TODO: Extend this to support showing how relationships types connect to nodes

    SchemaCalculator( KernelTransaction ktx )
    {
        this.ktx = ktx;
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
                for ( Integer propId : propertyIds )
                {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeDecider valueTypeDecider = relationshipTypeIdANDPropertyTypeIdToValueTypeMapping.get( Pair.of( typeId, propId ) );
                    results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), propName,
                            valueTypeDecider.getCypherTypeString() ) );
                }
            }
        }
        return results;
    }

    private List<SchemaInfoResult> produceResultsForNodes()
    {
        List<SchemaInfoResult> results = new ArrayList<>();
        for ( LabelSet labelSet : labelSetToPropertyKeysMapping.keySet() )
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
                for ( Integer propId : propertyIds )
                {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeDecider valueTypeDecider = labelSetANDNodePropertyKeyIdToValueTypeMapping.get( Pair.of( labelSet, propId ) );
                    results.add( new SchemaInfoResult( NODE, labelNames, propName, valueTypeDecider.getCypherTypeString() ) );
                }
            }
        }
        return results;
    }

    public Stream<SchemaProcedure.GraphResult> calculateGraphResultStream()
    {
        calculateSchema();
        //TODO finish this
        throw new NotImplementedException();
    }

    //TODO: Parallelize this: (Nodes | Rels) and/or more
    //TODO: If we would have this schema information in the count store (or somewhere), this could be super fast
    private void calculateSchema()
    {
        // this one does most of the work
        try ( Statement ignore = ktx.acquireStatement() )
        {
            Read dataRead = ktx.dataRead();
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            CursorFactory cursors = ktx.cursors();

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

            scanEverythingBelongingToNodes( dataRead, cursors );
            scanEverythingBelongingToRelationships( dataRead, cursors );

            // OTHER:
            // go through all labels
            addNamesToCollection( tokenRead.labelsGetAllTokens(), labelIdToLabelNameMapping );
            // go through all propertyKeys
            addNamesToCollection( tokenRead.propertyKeyGetAllTokens(), propertyIdToPropertylNameMapping );
            // go through all relationshipTypes
            addNamesToCollection( tokenRead.relationshipTypesGetAllTokens(), relationshipTypIdToRelationshipNameMapping );
        }
    }

    private void scanEverythingBelongingToRelationships( Read dataRead, CursorFactory cursors )
    {
        RelationshipScanCursor relationshipScanCursor = cursors.allocateRelationshipScanCursor();
        PropertyCursor propertyCursor = cursors.allocatePropertyCursor();
        dataRead.allRelationshipsScan( relationshipScanCursor );
        while ( relationshipScanCursor.next() )
        {
            int typeId = relationshipScanCursor.type();
            relationshipScanCursor.properties( propertyCursor );
            Set<Integer> propertyIds = new HashSet<>();  // is Set really the best fit here?

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

    private void scanEverythingBelongingToNodes( Read dataRead, CursorFactory cursors )
    {
        NodeCursor nodeCursor = cursors.allocateNodeCursor();
        PropertyCursor propertyCursor = cursors.allocatePropertyCursor();
        dataRead.allNodesScan( nodeCursor );
        while ( nodeCursor.next() )
        {
            // each node
            LabelSet labels = nodeCursor.labels();
            nodeCursor.properties( propertyCursor );
            Set<Integer> propertyIds = new HashSet<>();  // is Set really the best fit here?

            while ( propertyCursor.next() )
            {
                Value currentValue = propertyCursor.propertyValue();
                int propertyKeyId = propertyCursor.propertyKey();
                Pair<LabelSet,Integer> key = Pair.of( labels, propertyKeyId );
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
                    Pair<LabelSet,Integer> key = Pair.of( labels, id );
                    labelSetANDNodePropertyKeyIdToValueTypeMapping.get( key ).setNullable();
                } );
            }

            propertyIds.addAll( oldPropertyKeySet );
            labelSetToPropertyKeysMapping.put( labels, propertyIds );
        }
        nodeCursor.close();
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
        private Boolean isNullable = false;
        private String nullableSuffix = "";

        ValueTypeDecider( Value v )
        {
            if ( v == null )
            {
                throw new IllegalArgumentException();
            }
            this.concreteValue = v;
            this.valueGroup = v.valueGroup();
            this.valueStatus = VALUE;
        }

        private void setNullable( )
        {
                isNullable = true;
                nullableSuffix = NULLABLE;
        }

        /*
        This method translates an ValueTypeDecider into the correct String
        */
        String getCypherTypeString()
        {
            switch ( valueStatus )
            {
            case VALUE:
                return isNullable ? concreteValue.getTypeName().toUpperCase() + nullableSuffix
                                  : concreteValue.getTypeName().toUpperCase();
            case VALUE_GROUP:
                return isNullable ? valueGroup.name() + nullableSuffix
                                  : valueGroup.name();
            case ANY:
                return isNullable ? NULLABLE_ANYVALUE
                                  : ANYVALUE;
            default:
                throw new IllegalStateException( "Did not recognize ValueStatus" );
            }
        }

        /*
        This method is needed to handle conflicting property values and sets valueStatus accordingly to:
         A) VALUE if current and new value match on class level
         B) VALUE_GROUP, if at least the ValueGroups of the current and new values match
         C) ANY, if nothing matches
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
                // check if classes match
                if ( !concreteValue.getClass().equals( newValue.getClass() ) )
                {
                    // Clases don't match -> update needed
                    if ( valueGroup.equals( newValue.valueGroup() ) )
                    {
                        // same valueGroup -> set that
                        valueStatus = VALUE_GROUP;
                    }
                    else
                    {
                        // Not same valueGroup -> update to AnyValue
                        valueStatus = ANY;
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
        VALUE_GROUP,
        ANY
    }
}
