package org.neo4j.impl.command;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

/**
 * Holds nodes and relationships that participate in i transaction. 
 */
public class TransactionCache
{
	private static TransactionCache instance = new TransactionCache();
	
	// currentThread = key, value = Map
	private Map<Thread,Map<Integer,Node>> txToCacheMapNodes = 
		java.util.Collections.synchronizedMap( 
		new HashMap<Thread,Map<Integer,Node>>() );
	private Map<Thread,Map<Integer,Relationship>> txToCacheMapRels = 
		java.util.Collections.synchronizedMap( 
			new HashMap<Thread,Map<Integer,Relationship>>() );
	
	private TransactionCache()
	{}
	
	public static TransactionCache getCache()
	{
		return instance;
	}
	
	/**
	 * Adds a node to the current transaction.
	 * 
	 * @param node The node to add to the current transaction
	 */
	public void addNode( Node node )
	{
		int id = (int) node.getId();
		Thread currentThread = Thread.currentThread();
		if ( txToCacheMapNodes.containsKey(  currentThread ) )
		{
			Map<Integer,Node> cache = txToCacheMapNodes.get( currentThread );
			cache.put( id, node );
		}
		else if ( CommandManager.getManager().txCommitHookRegistered() )
		{
			Map<Integer,Node> cache = new HashMap<Integer,Node>();
			cache.put( id, node );
			txToCacheMapNodes.put( currentThread, cache );
		}
	}
	
	/**
	 * Adds a relationship to the current transaction.
	 * 
	 * @param rel The relationship to add to the transaction
	 */
	public void addRelationship( Relationship rel )
	{
		int id = (int) rel.getId();
		Thread currentThread = Thread.currentThread();
		if ( txToCacheMapRels.containsKey( currentThread ) )
		{
			Map<Integer,Relationship> cache = 
				txToCacheMapRels.get( currentThread );
			cache.put( id, rel );
		}
		else if ( CommandManager.getManager().txCommitHookRegistered() )
		{
			Map<Integer,Relationship> cache = 
				new HashMap<Integer,Relationship>();
			cache.put( id, rel );
			txToCacheMapRels.put( currentThread, cache );
		}
	}
	
	/**
	 * Returns a node if it is participating in the transaction.
	 * 
	 * @param id The id of the node
	 * @return The node if it exists or <CODE>null</CODE>
	 */
	public Node getNode( Integer id )
	{
		Thread currentThread = Thread.currentThread();
		Map<Integer,Node> nodeMap = txToCacheMapNodes.get( currentThread );
		if ( nodeMap != null )
		{
			return nodeMap.get( id );
		}
		return null;
	}
	
	/**
	 * Returns a relationship if it is participating in the transaction.
	 * 
	 * @param id The id of the relationship
	 * @return The relationship if it exists or <CODE>null</CODE>
	 */
	public Relationship getRelationship( Integer id )
	{
		Thread currentThread = Thread.currentThread();
		Map<Integer,Relationship> relMap = 
			txToCacheMapRels.get( currentThread );
		if ( relMap != null )
		{
			return relMap.get( id );
		}
		return null;
	}
	
	/**
	 * Removes all nodes and relationships added to the current transaction.
	 */
	public void cleanCurrentTransaction()
	{
		Thread currentThread = Thread.currentThread();
		txToCacheMapNodes.remove( currentThread );
		txToCacheMapRels.remove( currentThread );
	}
	
	String size()
	{
		StringBuffer string = new StringBuffer();
		string.append( "Node[" + txToCacheMapNodes.size() + "] Rels[" + 
			txToCacheMapRels.size() + "]" );
		java.util.Iterator itr = txToCacheMapNodes.keySet().iterator();
		while ( itr.hasNext() )
		{
			Object key = itr.next();
			string.append( "\nThread=" + key );
			java.util.Iterator innerItr = 
				( ( Map ) txToCacheMapNodes.get( key ) ).values().iterator();
			while ( innerItr.hasNext() ) 
			{
				string.append( "\n\t" + innerItr.next() );
			}
		}
		itr = txToCacheMapRels.keySet().iterator();
		while ( itr.hasNext() )
		{
			Object key = itr.next();
			string.append( "\nThread=" + key );
			java.util.Iterator innerItr = 
				( ( Map ) txToCacheMapRels.get( key ) ).values().iterator();
			while ( innerItr.hasNext() ) 
			{
				string.append( "\n\t" + innerItr.next() );
			}
		}
		return string.toString();
	}
}
