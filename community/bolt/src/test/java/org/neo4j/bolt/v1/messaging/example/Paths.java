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
package org.neo4j.bolt.v1.messaging.example;

import org.neo4j.values.virtual.PathValue;

import static org.neo4j.bolt.v1.messaging.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.v1.messaging.example.Edges.ALICE_LIKES_CAROL;
import static org.neo4j.bolt.v1.messaging.example.Edges.CAROL_DISLIKES_BOB;
import static org.neo4j.bolt.v1.messaging.example.Edges.CAROL_MARRIED_TO_DAVE;
import static org.neo4j.bolt.v1.messaging.example.Edges.DAVE_WORKS_FOR_DAVE;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Nodes.BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.CAROL;
import static org.neo4j.bolt.v1.messaging.example.Nodes.DAVE;
import static org.neo4j.bolt.v1.messaging.example.Support.edges;
import static org.neo4j.bolt.v1.messaging.example.Support.nodes;
import static org.neo4j.values.virtual.VirtualValues.path;

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
    public static final PathValue PATH_WITH_LENGTH_ZERO = path( nodes( ALICE ), edges() );

    public static final PathValue PATH_WITH_LENGTH_ONE =
            path( // A->B
                    nodes( ALICE, BOB ),
                    edges( ALICE_KNOWS_BOB ) );
    public static final PathValue PATH_WITH_LENGTH_TWO =
            path( // A->C->D
                    nodes( ALICE, CAROL, DAVE ),
                    edges( ALICE_LIKES_CAROL,
                            CAROL_MARRIED_TO_DAVE ) );
    public static final PathValue PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION =
            path( // A->B<-C->D
                    nodes( ALICE, BOB, CAROL, DAVE ),
                    edges( ALICE_KNOWS_BOB, CAROL_DISLIKES_BOB, CAROL_MARRIED_TO_DAVE ) );
    public static final PathValue PATH_WITH_NODES_VISITED_MULTIPLE_TIMES =
            path( // A->B<-A->C->B<-C
                    nodes( ALICE, BOB, ALICE, CAROL, BOB, CAROL),
                    edges( ALICE_KNOWS_BOB, ALICE_KNOWS_BOB, ALICE_LIKES_CAROL,
                            CAROL_DISLIKES_BOB, CAROL_DISLIKES_BOB ) );
    public static final PathValue PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION =
            path( // A->C->B<-A->C->D
                    nodes( ALICE, CAROL, BOB, ALICE, CAROL, DAVE ),
                    edges( ALICE_LIKES_CAROL, CAROL_DISLIKES_BOB, ALICE_KNOWS_BOB,ALICE_LIKES_CAROL,
                            CAROL_MARRIED_TO_DAVE ) );
    public static final PathValue PATH_WITH_LOOP =
            path( // C->D->D
                    nodes( CAROL, DAVE, DAVE ),
                    edges( CAROL_MARRIED_TO_DAVE, DAVE_WORKS_FOR_DAVE ) );

    public static final PathValue[] ALL_PATHS = new PathValue[]{
            PATH_WITH_LENGTH_ZERO,
            PATH_WITH_LENGTH_ONE,
            PATH_WITH_LENGTH_TWO,
            PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION,
            PATH_WITH_NODES_VISITED_MULTIPLE_TIMES,
            PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION,
            PATH_WITH_LOOP,
    };

    private Paths()
    {
    }
}
