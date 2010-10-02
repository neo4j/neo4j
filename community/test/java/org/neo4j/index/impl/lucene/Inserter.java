package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Inserter
{
	public static void main( String[] args )
	{
		String path = args[0];
		final GraphDatabaseService db = new EmbeddedGraphDatabase( path );
		final IndexProvider provider = new LuceneIndexProvider( db );
		final Index<Node> index = provider.nodeIndex( "myIndex", LuceneIndexProvider.EXACT_CONFIG );
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
						Transaction tx = db.beginTx();
						try
						{
							for ( int i = 0; i < 100; i++ )
							{
								Node node = db.createNode();
								index.add( node, keys[i%keys.length], values[i%values.length]+i );
							}
							tx.success();
						}
						finally
						{
							tx.finish();
						}
					}
				}
			}.start();
		}
	}
}
