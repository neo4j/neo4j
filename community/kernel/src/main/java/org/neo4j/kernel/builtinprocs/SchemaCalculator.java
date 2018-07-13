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

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

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
import org.neo4j.values.storable.Value;

public class SchemaCalculator
{
    private Map<SortedLabels,MutableIntSet> labelSetToPropertyKeysMapping;
    private Map<Pair<SortedLabels,Integer>,ValueTypeListHelper> labelSetANDNodePropertyKeyIdToValueTypeMapping;
    private Set<SortedLabels> nullableLabelSets; // used for label combinations without properties -> all properties are viewed as nullable
    private Map<Integer,String> labelIdToLabelNameMapping;
    private Map<Integer,String> propertyIdToPropertylNameMapping;
    private Map<Integer,String> relationshipTypIdToRelationshipNameMapping;
    private Map<Integer,MutableIntSet> relationshipTypeIdToPropertyKeysMapping;
    private Map<Pair<Integer,Integer>,ValueTypeListHelper> relationshipTypeIdANDPropertyTypeIdToValueTypeMapping;
    private Set<Integer> nullableRelationshipTypes; // used for types without properties -> all properties are viewed as nullable

    private final MutableIntSet emptyPropertyIdSet = IntSets.mutable.empty();
    private static final String NODE = "Node";
    private static final String RELATIONSHIP = "Relationship";

    private final Read dataRead;
    private final TokenRead tokenRead;
    private final CursorFactory cursors;

    SchemaCalculator( Transaction ktx )
    {
        this.dataRead = ktx.dataRead();
        this.tokenRead = ktx.tokenRead();
        this.cursors = ktx.cursors();

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
        nullableLabelSets = new HashSet<>(  );
        nullableRelationshipTypes = new HashSet<>(  );
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
            MutableIntSet propertyIds = relationshipTypeIdToPropertyKeysMapping.get( typeId );
            if ( propertyIds.size() == 0 )
            {
                results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), null, null, true ) );
            }
            else
            {
                propertyIds.forEach( propId -> {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeListHelper valueTypeListHelper = relationshipTypeIdANDPropertyTypeIdToValueTypeMapping.get( Pair.of( typeId, propId ) );
                    if ( nullableRelationshipTypes.contains( typeId ) )
                    {
                        results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), propName, valueTypeListHelper.getCypherTypesList(),
                                true ) );
                    }
                    else
                    {
                        results.add( new SchemaInfoResult( RELATIONSHIP, Collections.singletonList( name ), propName, valueTypeListHelper.getCypherTypesList(),
                                valueTypeListHelper.isNullable() ) );
                    }
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
            MutableIntSet propertyIds = labelSetToPropertyKeysMapping.get( labelSet );
            if ( propertyIds.size() == 0 )
            {
                results.add( new SchemaInfoResult( NODE, labelNames, null, null, true ) );
            }
            else
            {
                propertyIds.forEach( propId -> {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    ValueTypeListHelper valueTypeListHelper = labelSetANDNodePropertyKeyIdToValueTypeMapping.get( Pair.of( labelSet, propId ) );
                    if ( nullableLabelSets.contains( labelSet ) )
                    {
                        results.add( new SchemaInfoResult( NODE, labelNames, propName, valueTypeListHelper.getCypherTypesList(), true ) );
                    }
                    else
                    {
                        results.add( new SchemaInfoResult( NODE, labelNames, propName, valueTypeListHelper.getCypherTypesList(),
                                valueTypeListHelper.isNullable() ) );
                    }
                } );
            }
        }
        return results;
    }

    //TODO: If we would have this schema information in the count store (or somewhere), this could be super fast
    private void calculateSchema()
    {
        scanEverythingBelongingToNodes( );
        scanEverythingBelongingToRelationships( );

        // OTHER:
        // go through all labels
        addNamesToCollection( tokenRead.labelsGetAllTokens(), labelIdToLabelNameMapping );
        // go through all propertyKeys
        addNamesToCollection( tokenRead.propertyKeyGetAllTokens(), propertyIdToPropertylNameMapping );
        // go through all relationshipTypes
        addNamesToCollection( tokenRead.relationshipTypesGetAllTokens(), relationshipTypIdToRelationshipNameMapping );
    }

    private void scanEverythingBelongingToRelationships( )
    {
        try ( RelationshipScanCursor relationshipScanCursor = cursors.allocateRelationshipScanCursor();
                PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            dataRead.allRelationshipsScan( relationshipScanCursor );
            while ( relationshipScanCursor.next() )
            {
                int typeId = relationshipScanCursor.type();
                relationshipScanCursor.properties( propertyCursor );
                MutableIntSet propertyIds = IntSets.mutable.empty();

                while ( propertyCursor.next() )
                {
                    int propertyKey = propertyCursor.propertyKey();

                    Value currentValue = propertyCursor.propertyValue();
                    Pair<Integer,Integer> key = Pair.of( typeId, propertyKey );
                    updateValueTypeInMapping( currentValue, key, relationshipTypeIdANDPropertyTypeIdToValueTypeMapping );

                    propertyIds.add( propertyKey );
                }
                propertyCursor.close();

                MutableIntSet oldPropertyKeySet = relationshipTypeIdToPropertyKeysMapping.getOrDefault( typeId, emptyPropertyIdSet );

                // find out which old properties we did not visited and mark them as nullable
                if ( oldPropertyKeySet == emptyPropertyIdSet )
                {
                    if ( propertyIds.size() == 0 )
                    {
                        // Even if we find property key on other rels with this type, set all of them nullable
                        nullableRelationshipTypes.add( typeId );
                    }
                }
                else
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

    private void scanEverythingBelongingToNodes( )
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
                MutableIntSet propertyIds = IntSets.mutable.empty();

                while ( propertyCursor.next() )
                {
                    Value currentValue = propertyCursor.propertyValue();
                    int propertyKeyId = propertyCursor.propertyKey();
                    Pair<SortedLabels,Integer> key = Pair.of( labels, propertyKeyId );
                    updateValueTypeInMapping( currentValue, key, labelSetANDNodePropertyKeyIdToValueTypeMapping );

                    propertyIds.add( propertyKeyId );
                }
                propertyCursor.close();

                MutableIntSet oldPropertyKeySet = labelSetToPropertyKeysMapping.getOrDefault( labels, emptyPropertyIdSet );

                // find out which old properties we did not visited and mark them as nullable
                if ( oldPropertyKeySet == emptyPropertyIdSet )
                {
                    if ( propertyIds.size() == 0 )
                    {
                        // Even if we find property key on other nodes with those labels, set all of them nullable
                        nullableLabelSets.add( labels );
                    }
                }
                else
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

    private <X, Y> void updateValueTypeInMapping( Value currentValue, Pair<X,Y> key, Map<Pair<X,Y>,ValueTypeListHelper> mappingToUpdate )
    {
        ValueTypeListHelper helper = mappingToUpdate.get( key );
        if ( helper == null )
        {
            helper = new ValueTypeListHelper( currentValue );
            mappingToUpdate.put( key, helper );
        }
        else
        {
            helper.updateValueTypesWith( currentValue );
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

    private class ValueTypeListHelper
    {
        private Set<String> seenValueTypes;
        private boolean isNullable;

        ValueTypeListHelper( Value v )
        {
            seenValueTypes = new HashSet<>();
            updateValueTypesWith( v );
        }

        private void setNullable( )
        {
                isNullable = true;
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        List<String> getCypherTypesList()
        {
            return new ArrayList<>( seenValueTypes );
        }

        void updateValueTypesWith( Value newValue )
        {
            if ( newValue == null )
            {
                throw new IllegalArgumentException();
            }
            seenValueTypes.add( newValue.getTypeName() );
        }
    }
}
