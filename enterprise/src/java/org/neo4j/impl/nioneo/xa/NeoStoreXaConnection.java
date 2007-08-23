package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.neo4j.impl.core.PropertyIndex;
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
	 
	private final NeoStore neoStore;
	private final NodeEventConsumer nodeConsumer;
	private final RelationshipEventConsumer relConsumer;
	private final RelationshipTypeEventConsumer relTypeConsumer;
	private final PropertyIndexEventConsumer propIndexConsumer;
	
	private NeoTransaction neoTransaction = null;
	
	NeoStoreXaConnection( NeoStore neoStore, XaResourceManager xaRm )
	{
		super( xaRm );
		this.neoStore = neoStore;
		
		this.nodeConsumer = new NodeEventConsumerImpl( this ); 
		this.relConsumer = new RelationshipEventConsumerImpl( this ); 
		this.relTypeConsumer = new RelationshipTypeEventConsumerImpl( this );
		this.propIndexConsumer = new PropertyIndexEventConsumerImpl( this );
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
	
	public PropertyIndexEventConsumer getPropertyIndexConsumer()
	{
		return propIndexConsumer;
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
		if ( neoTransaction != null )
		{
			return neoTransaction;
		}
		try
		{
			neoTransaction = ( NeoTransaction ) getTransaction();
			return neoTransaction;
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
	
//	void validateXaConnection()
//	{
//		try
//		{
//			super.validate();
//		}
//		catch ( XAException e )
//		{
//			throw new RuntimeException( "Unable to validate " + e );
//		}
//	}
	
	private class NodeEventConsumerImpl implements NodeEventConsumer
	{
		private final NeoStoreXaConnection xaCon;
		private final NodeStore nodeStore;
		
		public NodeEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			nodeStore = getNodeStore();
		}
		
//		public void validate()
//		{
//			getNodeStore().validate();
//			getPropertyStore().validate();
//			validateXaConnection();
//		}
		
		public void createNode( int nodeId ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.NodeCreate( nodeId ) );
			xaCon.getNeoTransaction().nodeCreate( nodeId );
		}
		
		public void deleteNode( int nodeId ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.NodeDelete( nodeId ) );
			xaCon.getNeoTransaction().nodeDelete( nodeId );
		}
		
		// checks for created in tx else get from store
		public boolean loadLightNode( int nodeId ) throws IOException 
		{
//			validate();
			// if created in this tx should be in tx cache set as FULL
			// and then this should never be called
//			if ( xaCon.getNeoTransaction().nodeCreated( nodeId )  )
//			{
//				return true;
//			}
			// return nodeStore.loadLightNode( nodeId );
			return xaCon.getNeoTransaction().nodeLoadLight( nodeId );
		}
		
		// checks for created/deleted in tx else from store
//		public boolean checkNode( int nodeId ) throws IOException
//		{
////			validate();
//			if ( xaCon.getNeoTransaction().nodeCreated( nodeId ) && 
//				 !xaCon.getNeoTransaction().nodeDeleted( nodeId ) )
//			{
//				return true;
//			}
//			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) )
//			{
//				return false;
//			}
//			return nodeStore.loadLightNode( nodeId );
//		}
		
		public void addProperty( int nodeId, int propertyId, 
			PropertyIndex index, Object value ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.NodeAddProperty( nodeId, propertyId, key, 
//					value ) );
			xaCon.getNeoTransaction().nodeAddProperty( nodeId, propertyId, 
				index, value ); 
		}
	
		public void changeProperty( int nodeId, int propertyId, Object value ) 
			throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.NodeChangeProperty( nodeId, propertyId, 
//					value ) );
			xaCon.getNeoTransaction().nodeChangeProperty( propertyId, value );
		}
		
		public void removeProperty( int nodeId, int propertyId ) 
			throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.NodeRemoveProperty( nodeId, propertyId ) );
			xaCon.getNeoTransaction().nodeRemoveProperty( nodeId, propertyId ); 
		}
		
		public PropertyData[] getProperties( int nodeId ) throws IOException
		{
//			validate();
			// if created in this tx should be in tx cache set as FULL
			// and then this should never be called
//			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) ||
//				 xaCon.getNeoTransaction().nodeCreated( nodeId ) )
//			{
//				// created in this tx
//				// for now asume everything in memory
//				return new PropertyData[0];
//			}
			
//			PropertyData[] propertyData = nodeStore.getProperties( nodeId );
//			List<PropertyData> propertyDataList = 
//				new ArrayList<PropertyData>();
//			for ( int i = 0; i < propertyData.length; i++ )
//			{
////				int propertyId = propertyData[i].getId();
//				// if deleted in this tx should be in tx cache set as FULL
//				// and then this should never be called
////				if ( !xaCon.getNeoTransaction().propertyDeleted( propertyId ) )
////				{
//					propertyDataList.add( propertyData[i] );
////				}
//			}
//			if ( propertyDataList.size() == propertyData.length )
//			{
//				return propertyData;
//			}
//			return propertyDataList.toArray( 
//				new PropertyData[ propertyDataList.size() ] );
			return xaCon.getNeoTransaction().nodeGetProperties( nodeId );			
		}
		
		public RelationshipData[] getRelationships( int nodeId ) 
			throws IOException
		{
//			validate();
			// if created in this tx should be in tx cache set as FULL
			// and then this should never be called
//			if ( xaCon.getNeoTransaction().nodeDeleted( nodeId ) ||
//				 xaCon.getNeoTransaction().nodeCreated( nodeId ) )
//			{
//				// created in this tx
//				// for now asume everything in memory
//				return new RelationshipData[0];
//			}
			
//			RelationshipData relData[] = nodeStore.getRelationships( nodeId );
//			List<RelationshipData> relList = new ArrayList<RelationshipData>();
//			for ( int i = 0; i < relData.length; i++ )
//			{
//				// if created in this tx should be in tx cache set as FULL
//				// and then this should never be called
////				if ( !xaCon.getNeoTransaction().relationshipDeleted( 
////						relData[i].getId() ) )
////				{
//					relList.add( relData[i] );
////				}
//			}
//			// rels.addAll( xaCon.getNeoTransaction().getCreatedRelsForNode( 
//			//	new Integer( nodeId ) ) );
//			if ( relList.size() == relData.length  )
//			{
//				return relData;
//			}
//			return relList.toArray( new RelationshipData[ relList.size() ] );
			return xaCon.getNeoTransaction().nodeGetRelationships( nodeId );
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
		private final NeoStoreXaConnection xaCon;
		private final RelationshipStore relStore;
//		private PropertyStore propStore;
		
		public RelationshipEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			this.relStore = getRelationshipStore();
//			this.propStore = getPropertyStore();
		}
		
//		public void validate() throws IOException
//		{
//			relStore.validate();
//			propStore.validate();
//			try
//			{
//				xaCon.validate();
//			}
//			catch ( XAException e )
//			{
//				throw new IOException( "Unable to validate " + e );
//			}
//		}

		public void createRelationship( int id, int firstNode, int secondNode, 
			int type ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipCreate( id, firstNode, secondNode, 
//					type ) );
			xaCon.getNeoTransaction().relationshipCreate( id, firstNode, 
				secondNode, type );		
		}
		
		public void deleteRelationship( int id ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipDelete( id ) );
			xaCon.getNeoTransaction().relDelete( id );		
		}
	
		public void addProperty( int relId, int propertyId, PropertyIndex index, 
			Object value ) throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipAddProperty( relId, propertyId, 
//					key, value ) );
			xaCon.getNeoTransaction().relAddProperty( relId, propertyId, index, 
				value );		
		}
	
		public void changeProperty( int relId, int propertyId, Object value ) 
			throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipChangeProperty( relId, propertyId, 
//					value ) );
			xaCon.getNeoTransaction().relChangeProperty( propertyId, value );		
		}
		
		public void removeProperty( int relId, int propertyId ) 
			throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipRemoveProperty( relId, 
//					propertyId ) );
			xaCon.getNeoTransaction().relRemoveProperty( relId, propertyId );		
		}
		
		public PropertyData[] getProperties( int relId )
			throws IOException
		{
//			validate();
			
			// if created in this tx should be in tx cache set as FULL
			// and then this should never be called
//			if ( xaCon.getNeoTransaction().relationshipDeleted( relId ) ||
//				 xaCon.getNeoTransaction().relationshipCreated( relId ) )
//			{
//				// created in this tx
//				// for now asume everything in memory
//				return new PropertyData[0];
//			}
			
//			PropertyData[] propertyData = relStore.getProperties( relId );
//			List<PropertyData> propertyDataList = 
//				new ArrayList<PropertyData>();
//			for ( int i = 0; i < propertyData.length; i++ )
//			{
//				// if created in this tx should be in tx cache set as FULL
//				// and then this should never be called
////				int propertyId = propertyData[i].getId();
////				if ( !xaCon.getNeoTransaction().propertyDeleted( propertyId ) )
////				{
//					propertyDataList.add( propertyData[i] );
////				}
//			}
//			if ( propertyDataList.size() == propertyData.length )
//			{
//				return propertyData;
//			}
//			return propertyDataList.toArray( 
//				new PropertyData[ propertyDataList.size() ] );
			return xaCon.getNeoTransaction().relGetProperties( relId );
		}
	
		public RelationshipData getRelationship( int id ) throws IOException
		{
//			validate();
			// if created in this tx should be in tx cache set as FULL
			// and then this should never be called
//			if ( xaCon.getNeoTransaction().relationshipCreated( id ) )
//			{
//				return xaCon.getNeoTransaction().getCreatedRelationship( id );
//			}
			
			// return relStore.getRelationship( id );
			return xaCon.getNeoTransaction().relationshipLoad( id );
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
		private final NeoStoreXaConnection xaCon;
		private final RelationshipTypeStore relTypeStore;
		
		RelationshipTypeEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
			this.relTypeStore = getRelationshipTypeStore();
		}
		
//		public void validate() throws IOException
//		{
//			relTypeStore.validate();
//			try
//			{
//				xaCon.validate();
//			}
//			catch ( XAException e )
//			{
//				throw new IOException( "Unable to validate " + e );
//			}
//		}

		public void addRelationshipType( int id, String name ) 
			throws IOException
		{
//			validate();
//			xaCon.getNeoTransaction().addMemoryCommand( 
//				new MemCommand.RelationshipTypeAdd( id, name ) ); 
			xaCon.getNeoTransaction().relationshipTypeAdd( id, name );		
		}

		public RelationshipTypeData getRelationshipType( int id ) 
			throws IOException
		{
//			validate();
//			if ( xaCon.getNeoTransaction().relationshipTypeAdded( id ) )
//			{
//				throw new RuntimeException( "Uhm should be cached" );
//			}
			return relTypeStore.getRelationshipType( id );
		}
	
		public RelationshipTypeData[] getRelationshipTypes()
			throws IOException
		{
//			validate();
			return relTypeStore.getRelationshipTypes();
		}
		
		public int nextId() throws IOException
		{
			return relTypeStore.nextId();
		}
	};

	private class PropertyIndexEventConsumerImpl 
		implements PropertyIndexEventConsumer
	{
		private final NeoStoreXaConnection xaCon;
//		private PropertyStore propStore = null;
		
		PropertyIndexEventConsumerImpl( NeoStoreXaConnection xaCon )
		{
			this.xaCon = xaCon;
//			this.propStore = getPropertyStore();
		}
		
		public void createPropertyIndex( int id, String key ) 
			throws IOException
		{
			xaCon.getNeoTransaction().createPropertyIndex( id, key );		
		}

		public String getKeyFor( int id ) throws IOException
		{
			return xaCon.getNeoTransaction().getPropertyIndex( id );
		}
	
//		public int nextId() throws IOException
//		{
//			return p
//		}
	};
}
