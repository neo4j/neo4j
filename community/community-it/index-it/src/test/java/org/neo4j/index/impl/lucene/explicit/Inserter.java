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
package org.neo4j.index.impl.lucene.explicit;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.TestGraphDatabaseFactory;

public class Inserter
{
    private Inserter()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        File path = new File( args[0] );
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path )
                .newGraphDatabase();
        final Index<Node> index = getIndex( db );
        final String[] keys = new String[]{"apoc", "zion", "morpheus"};
        final String[] values = new String[]{"hej", "yo", "something", "just a value", "anything"};

        for ( int i = 0; i < 5; i++ )
        {
            new Thread( () -> {
                while ( true )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int i1 = 0; i1 < 100; i1++ )
                        {
                            String key = keys[i1 % keys.length];
                            String value = values[i1 % values.length] + i1;

                            Node node = db.createNode();
                            node.setProperty( key, value );
                            index.add( node, key, value );
                        }
                        tx.success();
                    }
                }
            } ).start();
        }
        new File( path, "started" ).createNewFile();
    }

    private static Index<Node> getIndex( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( "myIndex" );
            transaction.success();
            return index;
        }
    }
}
