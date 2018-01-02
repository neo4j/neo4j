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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TestConstantDirectionExpander extends TraversalTestBase
{
    private static enum Types implements RelationshipType
    {
        A, B
    }

    private Transaction tx;

    @Before
    public void createGraph()
    {
        /*
         *   (l)--[A]-->(m)--[A]-->(n)<--[A]--(o)<--[B]--(p)<--[B]--(q)
         */
        createGraph( "l A m", "m A n", "o A n", "p B o", "q B p" );
        tx = beginTx();
    }

    @After
    public void tearDown()
    {
        tx.close();
    }

    @Test
    public void pathWithConstantDirection()
    {
        Node l = getNodeWithName( "l" );
        expectPaths( getGraphDb().traversalDescription()
                .expand(
                        PathExpanders.forConstantDirectionWithTypes( Types.A ) )
                .traverse( l ), "l", "l,m", "l,m,n" );

        Node n = getNodeWithName( "n" );
        expectPaths( getGraphDb().traversalDescription()
                .expand(
                        PathExpanders.forConstantDirectionWithTypes( Types.A ) )
                .traverse( n ), "n", "n,m", "n,m,l", "n,o" );

        Node q = getNodeWithName( "q" );
        expectPaths( getGraphDb().traversalDescription()
                .expand(
                        PathExpanders.forConstantDirectionWithTypes( Types.B ) )
                .traverse( q ), "q", "q,p", "q,p,o" );

    }
}
