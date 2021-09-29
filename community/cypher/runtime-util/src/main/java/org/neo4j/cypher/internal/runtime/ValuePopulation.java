/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.EMPTY_TEXT_ARRAY;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.MISSING_NODE;

public final class ValuePopulation
{
    private ValuePopulation()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static AnyValue populate( AnyValue value,
                                     DbAccess dbAccess,
                                     NodeCursor nodeCursor,
                                     RelationshipScanCursor relCursor,
                                     PropertyCursor propertyCursor )
    {
     if ( value instanceof VirtualNodeValue )
        {
            return populate( (VirtualNodeValue) value, dbAccess, nodeCursor, propertyCursor );
        }
        else if ( value instanceof VirtualRelationshipValue )
        {
            return populate( (VirtualRelationshipValue) value, dbAccess, nodeCursor, relCursor, propertyCursor );
        }
        else if ( value instanceof VirtualPathValue )
        {
            return populate( (VirtualPathValue) value, dbAccess, nodeCursor, relCursor, propertyCursor );
        }
        else if ( value instanceof ListValue )
        {
            return populate( (ListValue) value, dbAccess, nodeCursor, relCursor, propertyCursor );
        }
        else if ( value instanceof MapValue )
        {
            return populate( (MapValue) value, dbAccess, nodeCursor, relCursor, propertyCursor );
        }
        else
        {
            return value;
        }
    }

    public static NodeValue populate( VirtualNodeValue value,
                                      DbAccess dbAccess,
                                      NodeCursor nodeCursor,
                                      PropertyCursor propertyCursor )
    {
        if ( value instanceof NodeEntityWrappingNodeValue )
        {
            NodeEntityWrappingNodeValue wrappingNodeValue = (NodeEntityWrappingNodeValue) value;
            wrappingNodeValue.populate( nodeCursor, propertyCursor );
            return wrappingNodeValue;
        }
        else if ( value instanceof NodeValue )
        {
            return (NodeValue) value;
        }
        else
        {
            return nodeValue( value.id(), dbAccess, nodeCursor, propertyCursor );
        }
    }

    public static RelationshipValue populate( VirtualRelationshipValue value,
                                              DbAccess dbAccess,
                                              NodeCursor nodeCursor,
                                              RelationshipScanCursor relCursor,
                                              PropertyCursor propertyCursor )
    {
        if ( value instanceof RelationshipEntityWrappingValue )
        {
            RelationshipEntityWrappingValue wrappingValue = (RelationshipEntityWrappingValue) value;
            wrappingValue.populate( relCursor, propertyCursor );
            return wrappingValue;
        }
        else if ( value instanceof RelationshipValue )
        {
            return (RelationshipValue) value;
        }
        else
        {
            return relationshipValue( value.id(), dbAccess, nodeCursor, relCursor, propertyCursor );
        }
    }

    public static PathValue populate( VirtualPathValue value,
                                      DbAccess dbAccess,
                                      NodeCursor nodeCursor,
                                      RelationshipScanCursor relCursor,
                                      PropertyCursor propertyCursor )
    {
        if ( value instanceof PathValue )
        {
            return (PathValue) value;
        }
        else
        {
            var nodeIds = value.nodeIds();
            var relIds = value.relationshipIds();
            var nodes = new NodeValue[nodeIds.length];
            var rels = new RelationshipValue[relIds.length];
            long payloadSize = 0;
            //we know that rels.length + 1 = nodes.length
            int i = 0;
            for ( ; i < rels.length; i++ )
            {
                NodeValue nodeValue = nodeValue( nodeIds[i],dbAccess, nodeCursor, propertyCursor );
                RelationshipValue relationshipValue = relationshipValue( relIds[i],dbAccess, nodeCursor, relCursor, propertyCursor );
                payloadSize += nodeValue.estimatedHeapUsage() + relationshipValue.estimatedHeapUsage();
                nodes[i] = nodeValue;
                rels[i] = relationshipValue;
            }
            NodeValue nodeValue = nodeValue( nodeIds[i],dbAccess, nodeCursor, propertyCursor );
            payloadSize += nodeValue.estimatedHeapUsage();
            nodes[i] = nodeValue;

            return VirtualValues.path( nodes, rels, payloadSize );
        }
    }

    public static MapValue populate( MapValue value,
                                     DbAccess dbAccess,
                                     NodeCursor nodeCursor,
                                     RelationshipScanCursor relCursor,
                                     PropertyCursor propertyCursor )
    {
        MapValueBuilder builder = new MapValueBuilder();
        value.foreach( ( key, anyValue ) ->
                               builder.add( key, populate( anyValue,dbAccess, nodeCursor, relCursor, propertyCursor ) ) );
        return builder.build();
    }

    public static ListValue populate( ListValue value,
                                      DbAccess dbAccess,
                                      NodeCursor nodeCursor,
                                      RelationshipScanCursor relCursor,
                                      PropertyCursor propertyCursor )
    {
        ListValueBuilder builder = ListValueBuilder.newListBuilder( value.size() );
        for ( AnyValue v : value )
        {
            builder.add( populate( v, dbAccess, nodeCursor, relCursor, propertyCursor ) );
        }
        return builder.build();
    }

    private static NodeValue nodeValue( long id,
                                        DbAccess dbAccess,
                                        NodeCursor nodeCursor,
                                        PropertyCursor propertyCursor )
    {
        dbAccess.singleNode( id, nodeCursor );

        if ( !nodeCursor.next() )
        {
            //the node has probably been deleted, we still return it but just a bare id
            return VirtualValues.nodeValue( id, EMPTY_TEXT_ARRAY, EMPTY_MAP );
        }
        else
        {
            nodeCursor.properties( propertyCursor );
            return VirtualValues.nodeValue( id, labels( dbAccess, nodeCursor.labels() ), properties( propertyCursor, dbAccess ) );
        }
    }

    private static RelationshipValue relationshipValue( long id,
                                                        DbAccess dbAccess,
                                                        NodeCursor nodeCursor,
                                                        RelationshipScanCursor relCursor,
                                                        PropertyCursor propertyCursor )
    {
        dbAccess.singleRelationship( id, relCursor );
        if ( !relCursor.next() )
        {
            //the relationship has probably been deleted, we still return it but just a bare id
            return VirtualValues.relationshipValue( id, MISSING_NODE, MISSING_NODE, EMPTY_STRING, EMPTY_MAP );
        }
        else
        {
            VirtualNodeValue start = VirtualValues.node( relCursor.sourceNodeReference() );
            VirtualNodeValue end = VirtualValues.node( relCursor.targetNodeReference() );
            relCursor.properties( propertyCursor );
            return VirtualValues.relationshipValue( id, start, end, Values.stringValue( dbAccess.relationshipTypeName( relCursor.type() ) ),
                                                    properties( propertyCursor, dbAccess ) );
        }
    }

    private static TextArray labels( DbAccess dbAccess, TokenSet labelsTokens )
    {
        String[] labels = new String[labelsTokens.numberOfTokens()];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = dbAccess.nodeLabelName( labelsTokens.token( i ) );
        }
        return Values.stringArray( labels );
    }

    private static MapValue properties( PropertyCursor propertyCursor, DbAccess dbAccess )
    {
        MapValueBuilder builder = new MapValueBuilder();
        while ( propertyCursor.next() )
        {
            builder.add( dbAccess.propertyKeyName( propertyCursor.propertyKey() ), propertyCursor.propertyValue() );
        }
        return builder.build();
    }
}
