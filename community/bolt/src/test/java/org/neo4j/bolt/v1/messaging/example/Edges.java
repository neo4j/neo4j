/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging.example;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.EdgeValue;

import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Nodes.BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.CAROL;
import static org.neo4j.bolt.v1.messaging.example.Nodes.DAVE;
import static org.neo4j.bolt.v1.messaging.example.Support.NO_PROPERTIES;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.edgeValue;
import static org.neo4j.values.virtual.VirtualValues.map;

public class Edges
{
    // Relationship types
    public static final TextValue KNOWS = stringValue( "KNOWS" );
    public static final TextValue LIKES = stringValue( "LIKES" );
    public static final TextValue DISLIKES = stringValue( "DISLIKES" );
    public static final TextValue MARRIED_TO = stringValue( "MARRIED_TO" );
    public static final TextValue WORKS_FOR = stringValue( "WORKS_FOR" );

    // Edges
    public static final EdgeValue ALICE_KNOWS_BOB =
            edgeValue( 12L, ALICE, BOB, KNOWS,
                    map( new String[]{"since"}, new AnyValue[]{longValue( 1999L )} ) );
    public static final EdgeValue ALICE_LIKES_CAROL = edgeValue( 13L, ALICE, CAROL, LIKES, NO_PROPERTIES );
    public static final EdgeValue CAROL_DISLIKES_BOB = edgeValue( 32L, CAROL, BOB, DISLIKES, NO_PROPERTIES );
    public static final EdgeValue CAROL_MARRIED_TO_DAVE = edgeValue( 34L, CAROL, DAVE, MARRIED_TO, NO_PROPERTIES );
    public static final EdgeValue DAVE_WORKS_FOR_DAVE = edgeValue( 44L, DAVE, DAVE, WORKS_FOR, NO_PROPERTIES );

    private Edges()
    {
    }
}
