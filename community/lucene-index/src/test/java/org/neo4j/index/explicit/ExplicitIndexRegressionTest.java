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
package org.neo4j.index.explicit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public class ExplicitIndexRegressionTest
{
    @Rule
    public final DatabaseRule graphdb = new ImpermanentDatabaseRule();

    @Test
    public void shouldAccessAndUpdateIndexInSameTransaction() throws Exception
    {
        try ( Transaction tx = graphdb.beginTx() )
        {
            Index<Node> version = graphdb.index().forNodes( "version" );
            for ( int v = 0; v < 10; v++ )
            {
                createNode( version, v );
            }
            tx.success();
        }
    }

    private void createNode( Index<Node> index, long version )
    {
        highest( "version", index.query( new QueryContext( "version:*" ) ) );
        {
            Node node = graphdb.createNode();
            node.setProperty( "version", version );
            index.add( node, "version", version );
        }
        {
            Node node = index.get( "version", version ).getSingle();
            Node current = highest( "version", index.get( "current", "current" ) );
            if ( current != null )
            {
                index.remove( current, "current" );
            }
            index.add( node, "current", "current" );
        }
    }

    private Node highest( String key, IndexHits<Node> query )
    {
        try ( IndexHits<Node> hits = query )
        {
            long highestValue = Long.MIN_VALUE;
            Node highestNode = null;
            while ( hits.hasNext() )
            {
                Node node = hits.next();
                long value = ((Number) node.getProperty( key )).longValue();
                if ( value > highestValue )
                {
                    highestValue = value;
                    highestNode = node;
                }
            }
            return highestNode;
        }
    }
}
