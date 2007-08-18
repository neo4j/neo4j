package org.neo4j.impl.command;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.util.ArrayMap;

/**
 * Holds nodes and relationships that participate in i transaction. 
 */
public class TransactionCache
{
	private static ArrayMap<Thread,TransactionCache> threadToTxMap = 
		new ArrayMap<Thread,TransactionCache>( 9, true, true );
	
	public static TransactionCache getCache()
	{
		Thread currentThread = Thread.currentThread();
		TransactionCache cache = threadToTxMap.get( currentThread );
		if ( cache != null )
		{
			return cache;
		}
		CommandManager cMgr = CommandManager.getManager();
		if ( !cMgr.txCommitHookRegistered() )
		{
			cMgr.registerTxCommitHook( currentThread );
		}
		cache = new TransactionCache();
		threadToTxMap.put( currentThread, cache );
		return cache;
	}
	
	public static void cleanCurrentTransaction()
	{
		Thread currentThread = Thread.currentThread();
		threadToTxMap.remove( currentThread );
	}
	
	private Map<Integer,Node> nodeCache = 
		new HashMap<Integer,Node>(); // 9, false, true );
	private Map<Integer,Relationship> relCache = 
		new HashMap<Integer,Relationship>(); // 9, false, true );
	
	private TransactionCache()
	{}
		
	/**
	 * Adds a node to the current transaction.
	 * 
	 * @param node The node to add to the current transaction
	 */
	public void addNode( Node node )
	{
		int id = (int) node.getId();
		nodeCache.put( id, node );
	}
	
	/**
	 * Adds a relationship to the current transaction.
	 * 
	 * @param rel The relationship to add to the transaction
	 */
	public void addRelationship( Relationship rel )
	{
		int id = (int) rel.getId();
		relCache.put( id, rel );
	}
	
	/**
	 * Returns a node if it is participating in the transaction.
	 * 
	 * @param id The id of the node
	 * @return The node if it exists or <CODE>null</CODE>
	 */
	public Node getNode( int id )
	{
		return nodeCache.get( id );
	}
	
	/**
	 * Returns a relationship if it is participating in the transaction.
	 * 
	 * @param id The id of the relationship
	 * @return The relationship if it exists or <CODE>null</CODE>
	 */
	public Relationship getRelationship( int id )
	{
		return relCache.get( id );
	}
	
	int nodeSize()
	{
		return nodeCache.size();
	}
	
	int relSize()
	{
		return relCache.size();
	}
		
	static String size()
	{
		StringBuffer string = new StringBuffer();
		string.append( "TransactionCaches[" + threadToTxMap.size() + "]" );
		Iterator<Thread> itr = threadToTxMap.keySet().iterator();
		while ( itr.hasNext() )
		{
			Thread key = itr.next();
			string.append( "\nThread=" + key );
			TransactionCache cache = threadToTxMap.get( key );
			string.append( "\n\tNodes[" + cache.nodeSize() + " Relationships[" +
				cache.relSize() + "]" );
		}
		return string.toString();
	}
}
