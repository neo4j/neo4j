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
package org.neo4j.bolt.v1.messaging.example;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueRelationship;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.bolt.v1.messaging.example.Support.NO_PROPERTIES;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Nodes.BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.CAROL;
import static org.neo4j.bolt.v1.messaging.example.Nodes.DAVE;

public class Relationships
{
    // Relationship types
    public static final RelationshipType KNOWS = RelationshipType.withName( "KNOWS" );
    public static final RelationshipType LIKES = RelationshipType.withName( "LIKES" );
    public static final RelationshipType DISLIKES = RelationshipType.withName( "DISLIKES" );
    public static final RelationshipType MARRIED_TO =
            RelationshipType.withName( "MARRIED_TO" );
    public static final RelationshipType WORKS_FOR =
            RelationshipType.withName( "WORKS_FOR" );

    // Relationships
    public static final Relationship ALICE_KNOWS_BOB =
            new ValueRelationship( 12L, ALICE.getId(), BOB.getId(), KNOWS,
                    map( "since", 1999L ) );
    public static final Relationship ALICE_LIKES_CAROL =
            new ValueRelationship( 13L, ALICE.getId(), CAROL.getId(), LIKES,
                    NO_PROPERTIES );
    public static final Relationship CAROL_DISLIKES_BOB =
            new ValueRelationship( 32L, CAROL.getId(), BOB.getId(), DISLIKES,
                    NO_PROPERTIES );
    public static final Relationship CAROL_MARRIED_TO_DAVE =
            new ValueRelationship( 34L, CAROL.getId(), DAVE.getId(), MARRIED_TO,
                    NO_PROPERTIES );
    public static final Relationship DAVE_WORKS_FOR_DAVE =
            new ValueRelationship( 44L, DAVE.getId(), DAVE.getId(), WORKS_FOR,
                    NO_PROPERTIES );

}
