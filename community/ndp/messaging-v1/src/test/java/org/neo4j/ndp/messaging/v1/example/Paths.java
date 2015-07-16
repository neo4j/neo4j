/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1.example;

import org.neo4j.graphdb.Path;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;

import static org.neo4j.ndp.messaging.v1.example.Support.nodes;
import static org.neo4j.ndp.messaging.v1.example.Support.relationships;
import static org.neo4j.ndp.messaging.v1.example.Support.sequence;
import static org.neo4j.ndp.messaging.v1.example.Nodes.ALICE;
import static org.neo4j.ndp.messaging.v1.example.Nodes.BOB;
import static org.neo4j.ndp.messaging.v1.example.Nodes.CAROL;
import static org.neo4j.ndp.messaging.v1.example.Nodes.DAVE;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_LIKES_CAROL;
import static org.neo4j.ndp.messaging.v1.example.Relationships.CAROL_DISLIKES_BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.CAROL_MARRIED_TO_DAVE;
import static org.neo4j.ndp.messaging.v1.example.Relationships.DAVE_WORKS_FOR_DAVE;

/*
 * This class contains a number of paths used for testing, all based on
 * the following graph:
 * <pre>
 *
 *     (Bob)<--[:DISLIKES]---,
 *       ^                   |
 *       |                   |
 *    [:KNOWS]               |
 *       |                   |
 *       |                   |
 *     (Alice)--[:LIKES]-->(Carol)--[:MARRIED_TO]-->(Dave)-------------,
 *                                                    ^                |
 *                                                    |                |
 *                                                    '--[:WORKS_FOR]--'
 *
 * </pre>
*/
public class Paths
{
    // Paths
    public static final Path PATH_WITH_LENGTH_ZERO =
            new ValuePath( // A
                    nodes( ALICE ),
                    relationships(),
                    sequence( /* 0 */ ) );
    public static final Path PATH_WITH_LENGTH_ONE =
            new ValuePath( // A->B
                    nodes( ALICE, BOB ),
                    relationships( ALICE_KNOWS_BOB ),
                    sequence( /* 0 */ +1, 1 ) );
    public static final Path PATH_WITH_LENGTH_TWO =
            new ValuePath( // A->C->D
                    nodes( ALICE, CAROL, DAVE ),
                    relationships( ALICE_LIKES_CAROL,
                            CAROL_MARRIED_TO_DAVE ),
                    sequence( /* 0 */ +1, 1, +2, 2 ) );
    public static final Path PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION =
            new ValuePath( // A->B<-C->D
                    nodes( ALICE, BOB, CAROL, DAVE ),
                    relationships( ALICE_KNOWS_BOB, CAROL_DISLIKES_BOB, CAROL_MARRIED_TO_DAVE ),
                    sequence( /* 0 */ +1, 1, -2, 2, +3, 3 ) );
    public static final Path PATH_WITH_NODES_VISITED_MULTIPLE_TIMES =
            new ValuePath( // A->B<-A->C->B<-C
                    nodes( ALICE, BOB, CAROL ),
                    relationships( ALICE_KNOWS_BOB, ALICE_LIKES_CAROL,
                            CAROL_DISLIKES_BOB ),
                    sequence( /* 0 */ +1, 1, -1, 0, +2, 2, +3, 1, -3, 2 ) );
    public static final Path PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION =
            new ValuePath( // A->C->B<-A->C->D
                    nodes( ALICE, BOB, CAROL, DAVE ),
                    relationships( ALICE_LIKES_CAROL, CAROL_DISLIKES_BOB, ALICE_KNOWS_BOB,
                            CAROL_MARRIED_TO_DAVE ),
                    sequence( /* 0 */ +1, 2, +2, 1, -3, 0, +1, 2, +4, 3 ) );
    public static final Path PATH_WITH_LOOP =
            new ValuePath( // C->D->D
                    nodes( CAROL, DAVE ),
                    relationships( CAROL_MARRIED_TO_DAVE, DAVE_WORKS_FOR_DAVE ),
                    sequence( /* 0 */ +1, 1, +2, 1 ) );

    public static final Path[] ALL_PATHS = new Path[] {
            PATH_WITH_LENGTH_ZERO,
            PATH_WITH_LENGTH_ONE,
            PATH_WITH_LENGTH_TWO,
            PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION,
            PATH_WITH_NODES_VISITED_MULTIPLE_TIMES,
            PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION,
            PATH_WITH_LOOP,
    };

}
