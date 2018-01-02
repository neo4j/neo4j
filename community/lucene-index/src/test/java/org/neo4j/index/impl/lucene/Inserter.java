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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;

public class Inserter
{
	public static void main( String[] args ) throws IOException
	{
		String path = args[0];
		final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(path );
		final Index<Node> index = getIndex( db );
		final String[] keys = new String[] { "apoc", "zion", "morpheus" };
		final String[] values = new String[] { "hej", "yo", "something", "just a value", "anything" };
		
		for ( int i = 0; i < 5; i++ )
		{
			new Thread()
			{
				@Override
				public void run()
				{
					while ( true )
					{
						try ( Transaction tx = db.beginTx() )
						{
							for ( int i = 0; i < 100; i++ )
							{
                                String key = keys[i%keys.length];
                                String value = values[i%values.length]+i;
                                
								Node node = db.createNode();
                                node.setProperty( key, value );
								index.add( node, key, value );
							}
							tx.success();
						}
					}
				}
			}.start();
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
