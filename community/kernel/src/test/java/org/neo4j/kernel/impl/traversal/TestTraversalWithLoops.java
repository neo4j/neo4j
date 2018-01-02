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
package org.neo4j.kernel.impl.traversal;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Uniqueness;

import static org.neo4j.kernel.Traversal.traversal;

public class TestTraversalWithLoops extends TraversalTestBase
{
    @Test
    public void traverseThroughNodeWithLoop() throws Exception
    {
        /*
         * (a)-->(b)-->(c)-->(d)-->(e)
         *             /  \ /  \
         *             \__/ \__/
         */
        
        createGraph( "a TO b", "b TO c", "c TO c", "c TO d", "d TO d", "d TO e" );

        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( "a" );
            final Node e = getNodeWithName( "e" );
            Evaluator onlyEndNode = new Evaluator()
            {
                @Override
                public Evaluation evaluate( Path path )
                {
                    return Evaluation.ofIncludes( path.endNode().equals( e ) );
                }
            };
            TraversalDescription basicTraverser = traversal().evaluator( onlyEndNode );
            expectPaths( basicTraverser.traverse( a ), "a,b,c,d,e" );
            expectPaths( basicTraverser.uniqueness( Uniqueness.RELATIONSHIP_PATH ).traverse( a ),
                    "a,b,c,d,e", "a,b,c,c,d,e", "a,b,c,d,d,e", "a,b,c,c,d,d,e" );
            tx.success();
        }
    }
}
