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
package org.neo4j.cypher.internal.runtime;

import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

/**
 * Used to expose db access to expressions
 */
public interface DbAccess
{
    NodeValue nodeById( long id );

    RelationshipValue relationshipById( long id );

    Value nodeProperty( long node, int property );

    Value nodeProperty( long node, String propertyKey );

    int[] nodePropertyIds( long node );

    boolean nodeHasProperty( long node, int property );

    boolean nodeHasProperty( long node, String propertyKey );

    Value relationshipProperty( long node, int property );

    Value relationshipProperty( long node, String propertyKey );

    int[] relationshipPropertyIds( long node );

    boolean relationshipHasProperty( long node, int property );

    boolean relationshipHasProperty( long node, String propertyKey );

    int nodeGetOutgoingDegree( long node );

    int nodeGetOutgoingDegree( long node, String relationship );

    int nodeGetOutgoingDegree( long node, int relationship );

    int nodeGetIncomingDegree( long node );

    int nodeGetIncomingDegree( long node, String relationship );

    int nodeGetIncomingDegree( long node, int relationship );

    int nodeGetTotalDegree( long node );

    int nodeGetTotalDegree( long node, String relationship );

    int nodeGetTotalDegree( long node, int relationship );

    NodeValue relationshipGetStartNode( RelationshipValue relationship );

    NodeValue relationshipGetEndNode( RelationshipValue relationship );

    ListValue getLabelsForNode( long id );

    String getPropertyKeyName( int token );

    MapValue nodeAsMap( long id );

    MapValue relationshipAsMap( long id );

}
