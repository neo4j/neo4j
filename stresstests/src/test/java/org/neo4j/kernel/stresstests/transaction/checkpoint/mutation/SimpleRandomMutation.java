/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;

class SimpleRandomMutation implements RandomMutation
{
    private final long nodeCount;
    private final GraphDatabaseService db;
    private final Mutation rareMutation;
    private final Mutation commonMutation;

    public SimpleRandomMutation( long nodeCount, GraphDatabaseService db,
            Mutation rareMutation, Mutation commonMutation )
    {
        this.nodeCount = nodeCount;
        this.db = db;
        this.rareMutation = rareMutation;
        this.commonMutation = commonMutation;
    }

    private static String[] NAMES =
            {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s"};

    @Override
    public void perform()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long nodeId = rng.nextLong( nodeCount );
        String value = NAMES[rng.nextInt( NAMES.length )];

        if ( rng.nextDouble() < 0.01 )
        {
            rareMutation.perform( nodeId, value );
        }
        else
        {
            commonMutation.perform( nodeId, value );
        }
    }
}
