package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.impl.nioneo.store.IdGenerator;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceManager;

/**
 * {@link XaConnection} implementation for the NioNeo data store. Contains
 * getter methods for the different stores (node,relationship,property and
 * relationship type).
 * <p>
 * A <CODE>NeoStoreXaConnection</CODE> is obtained from 
 * {@link NeoStoreXaDataSource} and then Neo persistence layer can perform 
 * the operations requested via the store implementations.  
 */
public class NeoStoreXaConnection extends XaConnectionHelpImpl
{
	
	private NeoStoreXaResource xaResource = null;
	 
	private NeoStore neoStore;
	private NodeEventConsumer nodeConsumer = null;
	private RelationshipEventConsumer relConsumer = null;
	private RelationshipTypeEventConsumer relTypeConsumer = null;
	
	NeoStoreXaConnection( NeoStore neoStore, XaResourceManager xaRm )
	{
		super( xaRm );
		this.neoStore = neoStore;
		
		this.nodeConsumer = new NodeEventConsumerImpl( this ); 
		this.relConsumer = new RelationshipEventConsumerImpl( this ); 
		this.relTypeConsumer = new RelationshipTypeEventConsumerImpl( this ); 
		this.xaResource = new NeoStoreXaResource( xaRm );
	}

	/**
	 * Returns this neo store's {@link NodeStore}.
	 * 
	 * @return The node store
	 */
	public NodeEventConsumer getNodeConsumer()
	{
		return nodeConsumer;
	}
	
	/**
	 * Returns this neo store's {@link RelationshipStore}.
	 * 
	 * @return The relationship store
	 */
	public RelationshipEventConsumer getRelationshipConsumer()
	{
		return relConsumer;
	}
	
	/**
	 * Returns this neo store's {@link RelationshipTypeStore}.
	 * 
	 * @return The relationship type store
	 */
	public RelationshipTypeEventConsumer getRelationshipTypeConsumer()
	{
		return relTypeConsumer;
	}
	/**
	 * Made public for testing, dont use.
	 */
	public PropertyStore getPropertyStore()
	{
		return neoStore.getPropertyStore();
	}
	
	NodeStore getNodeStore()
	{
		return neoStore.getNodeStore();
	}

	RelationshipStore getRelationshipStore()
	{
		return neoStore.getRelationshipStore();
	}
	
	RelationshipTypeStore getRelationshipTypeStore()
	{
		return neoStore.getRelationshipTypeStore();
	}
	
	public XAResource getXaResource()
	{
		return this.xaResource;
	}
	
	NeoTransaction getNeoTransaction() throws IOException
	{
		try
		{
			return ( NeoTransaction ) getTransaction();
		}
		catch ( XAException e )
		{
			throw new IOException( "Unable to get transaction, " + e );
		}
	}
	
	private static class NeoStoreXaResource extends XaResourceHelpImpl
	{
		NeoStoreXaResource( XaResourceManager xaRm )
		{
			super( xaRm );
		}
		
		public boolean isSameRM( XAResource xares )
		{
			if ( xares instanceof NeoStoreXaResource )
			{
				// check for same store here later?
				return true;
			}
			return false;
		}		
	};
	
	private void validateXaConnection()
	{
		try
		{
			super.validate();
		}
		catch ( XAException e )
		{
			throw new RuntimeException( "Unable to validate " + e );
		}
	}
	
	private class NodeEventConsumerImpl implements NodeEventConsumer
	{
		private NeoStoreXaConnection xaCon;
		private NodeStore nodeStore;
		
		public NodeEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			nodeStore = getNodeStore();
		}
		
		public void validate()
		{
			getNodeStore().validate();
			getPropertyStore().validate();
			validateXaConnection();
		}
		
		public void createNode( int nodeId ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.NodeCreate( nodeId ) );
		}
		
		public void deleteNode( int nodeId ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.NodeDelete( nodeId ) );
		}
		
		// checks for created in tx else get from store
		public boolean loadLightNode( int nodeId ) throws IOException 
		{
			validate();
			if ( xaCon.getNeoTransaction().nodeCreated( nodeId )  )
			{
				return true;
			}
			return nodeStore.loadLightNode( nodeId );
		}
		
		// checks for created/deleted in tx else from store
		public boolean checkNode( int nodeId ) throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().nodeCreated( nodeId ) && 
				 !xaCon.getNeoTransaction().nodeDeleted( nodeId ) )
			{
				return true;
			}
			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) )
			{
				return false;
			}
			return nodeStore.loadLightNode( nodeId );
		}
		
		public void addProperty( int nodeId, int propertyId, String key, 
			Object value ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.NodeAddProperty( nodeId, propertyId, key, 
					value ) );
		}
	
		public void changeProperty( int nodeId, int propertyId, Object value ) 
			throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.NodeChangeProperty( nodeId, propertyId, 
					value ) );
		}
		
		public void removeProperty( int nodeId, int propertyId ) 
			throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.NodeRemoveProperty( nodeId, propertyId ) );
		}
		
		public PropertyData[] getProperties( int nodeId ) throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) ||
				 xaCon.getNeoTransaction().nodeCreated( nodeId ) )
			{
				// created in this tx
				// for now asume everything in memory
				return new PropertyData[0];
			}
			PropertyData[] propertyData = nodeStore.getProperties( nodeId );
			List<PropertyData> propertyDataList = 
				new ArrayList<PropertyData>();
			for ( int i = 0; i < propertyData.length; i++ )
			{
				int propertyId = propertyData[i].getId();
				if ( !xaCon.getNeoTransaction().propertyDeleted( propertyId ) )
				{
					propertyDataList.add( propertyData[i] );
				}
			}
			if ( propertyDataList.size() == propertyData.length )
			{
				return propertyData;
			}
			return propertyDataList.toArray( 
				new PropertyData[ propertyDataList.size() ] );
		}
		
		public RelationshipData[] getRelationships( int nodeId ) 
			throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) ||
				 xaCon.getNeoTransaction().nodeCreated( nodeId ) )
			{
				// created in this tx
				// for now asume everything in memory
				return new RelationshipData[0];
			}
			RelationshipData relData[] = nodeStore.getRelationships( nodeId );
			List<RelationshipData> relList = new ArrayList<RelationshipData>();
			for ( int i = 0; i < relData.length; i++ )
			{
				if ( !xaCon.getNeoTransaction().relationshipDeleted( 
						relData[i].getId() ) )
				{
					relList.add( relData[i] );
				}
			}
			// rels.addAll( xaCon.getNeoTransaction().getCreatedRelsForNode( 
			//	new Integer( nodeId ) ) );
			if ( relList.size() == relData.length  )
			{
				return relData;
			}
			return relList.toArray( new RelationshipData[ relList.size() ] );
		}
		
		public int nextId() throws IOException
		{
			return nodeStore.nextId();
		}
		
		public IdGenerator getIdGenerator()
		{
			throw new RuntimeException( "don't use" );
		}
	};		
	
	private class RelationshipEventConsumerImpl implements 
		RelationshipEventConsumer
	{
		private NeoStoreXaConnection xaCon;
		private RelationshipStore relStore;
		private PropertyStore propStore;
		
		public RelationshipEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			this.relStore = getRelationshipStore();
			this.propStore = getPropertyStore();
		}
		
		public void validate() throws IOException
		{
			relStore.validate();
			propStore.validate();
			try
			{
				xaCon.validate();
			}
			catch ( XAException e )
			{
				throw new IOException( "Unable to validate " + e );
			}
		}

		public void createRelationship( int id, int firstNode, int secondNode, 
			int type ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipCreate( id, firstNode, secondNode, 
					type ) );
		}
		
		public void deleteRelationship( int id ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipDelete( id ) );
		}
	
		public void addProperty( int relId, int propertyId, String key, 
			Object value ) throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipAddProperty( relId, propertyId, 
					key, value ) );
		}
	
		public void changeProperty( int relId, int propertyId, Object value ) 
			throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipChangeProperty( relId, propertyId, 
					value ) );
		}
		
		public void removeProperty( int relId, int propertyId ) 
			throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipRemoveProperty( relId, 
					propertyId ) );
		}
		
		public PropertyData[] getProperties( int relId )
			throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().relationshipDeleted( relId ) ||
				 xaCon.getNeoTransaction().relationshipCreated( relId ) )
			{
				// created in this tx
				// for now asume everything in memory
				return new PropertyData[0];
			}
			PropertyData[] propertyData = relStore.getProperties( relId );
			List<PropertyData> propertyDataList = 
				new ArrayList<PropertyData>();
			for ( int i = 0; i < propertyData.length; i++ )
			{
				int propertyId = propertyData[i].getId();
				if ( !xaCon.getNeoTransaction().propertyDeleted( propertyId ) )
				{
					propertyDataList.add( propertyData[i] );
				}
			}
			if ( propertyDataList.size() == propertyData.length )
			{
				return propertyData;
			}
			return propertyDataList.toArray( 
				new PropertyData[ propertyDataList.size() ] );
			
		}
	
		public RelationshipData getRelationship( int id ) throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().relationshipCreated( id ) )
			{
				return xaCon.getNeoTransaction().getCreatedRelationship( id );
			}
			return relStore.getRelationship( id );
		}
		
		public int nextId() throws IOException
		{
			return relStore.nextId();
		}

		public IdGenerator getIdGenerator()
		{
			throw new RuntimeException( "don't use" );
		}
	};

	private class RelationshipTypeEventConsumerImpl 
		implements RelationshipTypeEventConsumer
	{
		private NeoStoreXaConnection xaCon = null;
		private RelationshipTypeStore relTypeStore = null;
		
		RelationshipTypeEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			this.relTypeStore = getRelationshipTypeStore();
		}
		
		public void validate() throws IOException
		{
			relTypeStore.validate();
			try
			{
				xaCon.validate();
			}
			catch ( XAException e )
			{
				throw new IOException( "Unable to validate " + e );
			}
		}

		public void addRelationshipType( int id, String name ) 
			throws IOException
		{
			validate();
			xaCon.getNeoTransaction().addMemoryCommand( 
				new MemCommand.RelationshipTypeAdd( id, name ) ); 
		}

		public RelationshipTypeData getRelationshipType( int id ) 
			throws IOException
		{
			validate();
			if ( xaCon.getNeoTransaction().relationshipTypeAdded( id ) )
			{
				throw new RuntimeException( "Uhm should be cached" );
			}
			return relTypeStore.getRelationshipType( id );
		}
	
		public RelationshipTypeData[] getRelationshipTypes()
			throws IOException
		{
			validate();
			return relTypeStore.getRelationshipTypes();
		}
		
		public int nextId() throws IOException
		{
			return relTypeStore.nextId();
		}
		
		public int nextBlockId()
		{
			throw new RuntimeException( "Do not use" );
		}
		
		public void freeBlockId( int dontUse )
		{
			throw new RuntimeException( "Do not use" );
		}

		public IdGenerator getIdGenerator()
		{
			throw new RuntimeException( "don't use" );
		}
	};
}
