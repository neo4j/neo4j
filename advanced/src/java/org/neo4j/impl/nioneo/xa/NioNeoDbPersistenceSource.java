package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import javax.transaction.xa.XAResource;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.NodeOperationEventData;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;
import org.neo4j.impl.core.RelationshipOperationEventData;
import org.neo4j.impl.core.RelationshipTypeOperationEventData;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.persistence.PersistenceException;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.persistence.PersistenceSource;
import org.neo4j.impl.persistence.PersistenceUpdateFailedException;
import org.neo4j.impl.persistence.ResourceConnection;
import org.neo4j.impl.transaction.XaDataSourceManager;

/**
 * The NioNeo persistence source implementation. If this class is registered
 * as persistence source for Neo operations that are performed on the node
 * space will be forwared to this class {@link ResourceConnection} 
 * implementation. 
 */
public class NioNeoDbPersistenceSource implements PersistenceSource
{
	private static final String	MODULE_NAME	= "NioNeoDbPersistenceSource";

	private NeoStoreXaDataSource xaDs = null;
	private String dataSourceName = null;

	public synchronized void init()
	{
		// Do nothing
	}

	public synchronized void start()
	{
		xaDs = ( NeoStoreXaDataSource ) 
			XaDataSourceManager.getManager().getXaDataSource( "nioneodb" );
		if ( xaDs == null )
		{
			throw new RuntimeException( "Unable to get nioneodb datasource" );
		}
		if ( !EventManager.getManager().generateProActiveEvent(
			Event.DATA_SOURCE_ADDED, new EventData( this ) ) )
		{
			throw new RuntimeException( 
				"Unable to register NioNeoDbPersistenceSource" );
		}
	}

	public synchronized void reload()
	{
		// Do nothing
	}

	public synchronized void stop()
	{
		if ( xaDs != null )
		{			
			xaDs.close();
		}
		EventManager.getManager().generateReActiveEvent(
			Event.DATA_SOURCE_REMOVED, new EventData( this ) );
	}

	public synchronized void destroy()
	{
		// Do nothing
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
	
	public synchronized ResourceConnection createResourceConnection()
	{
		return new NioNeoDbResourceConnection( this.xaDs ); 
	}
	
	private static class NioNeoDbResourceConnection 
		implements ResourceConnection 
	{
		private NeoStoreXaConnection xaCon = null;
		private NodeEventConsumer nodeConsumer = null;
		private RelationshipEventConsumer relConsumer = null;
		private RelationshipTypeEventConsumer relTypeConsumer = null;
		private PropertyStore propStore = null;
		
		NioNeoDbResourceConnection( NeoStoreXaDataSource xaDs )
		{
			this.xaCon = ( NeoStoreXaConnection ) xaDs.getXaConnection();
			nodeConsumer = xaCon.getNodeConsumer();
			relConsumer = xaCon.getRelationshipConsumer();
			relTypeConsumer = xaCon.getRelationshipTypeConsumer();
			propStore = xaCon.getPropertyStore();
		}
		
		public XAResource getXAResource()
		{
			return this.xaCon.getXaResource();
		}
		
		public void destroy()
		{
			xaCon.destroy();
			xaCon = null;
			nodeConsumer = null;
			relConsumer = null;
			relTypeConsumer = null;
		}
		
		public Object performOperation( PersistenceManager.Operation operation, 
			Object param ) throws PersistenceException
		{
			try
			{
				if ( operation == 
						PersistenceManager.LOAD_ALL_RELATIONSHIP_TYPES )
				{
					RelationshipTypeData relTypeData[] = 
						relTypeConsumer.getRelationshipTypes();
					RawRelationshipTypeData rawRelTypeData[] = 
						new RawRelationshipTypeData[ relTypeData.length ];
					for ( int i = 0; i < relTypeData.length; i++ )
					{
						rawRelTypeData[i] = new RawRelationshipTypeData( 
							relTypeData[i].getId(), 
							relTypeData[i].getName() );
					}
					return rawRelTypeData;
				}
				else if ( operation == PersistenceManager.LOAD_LIGHT_NODE )
				{
					if ( nodeConsumer.loadLightNode( 
							( ( Integer ) param ).intValue() ) )
					{
						return new RawNodeData();
					}
					else
					{
						return null;
					}
				}
				else if ( operation == 
					PersistenceManager.LOAD_NODE_PROPERTIES )
				{
					int id = (int) ( ( Node ) param ).getId();
					PropertyData propData[] = nodeConsumer.getProperties( id );
					RawPropertyData properties[] = 
						new RawPropertyData[ propData.length ];
					for ( int i = 0; i < propData.length; i++ )
					{
						properties[i] = new RawPropertyData(
							propData[i].getId(), 
							propData[i].getKey(), 
							propData[i].getValue() );
					}
					return properties;
					
				}
				else if ( operation == PersistenceManager.LOAD_RELATIONSHIPS )
				{
					int id = (int) ( ( Node ) param ).getId();
					RelationshipData relData[] = 
						nodeConsumer.getRelationships( id );
					RawRelationshipData relationships[] = 
						new RawRelationshipData[ relData.length ];
					for ( int i = 0; i < relData.length; i++ )
					{
						relationships[i] = new RawRelationshipData( 
							relData[i].getId(), 
							relData[i].firstNode(), 
							relData[i].secondNode(),
							relData[i].relationshipType() );
					}
					return relationships;
				}
				else if ( operation == PersistenceManager.LOAD_LIGHT_REL )
				{
					int id = ( ( Integer ) param ).intValue();
					RelationshipData relData = 
						relConsumer.getRelationship( id );
					
					// catch not found exception here and return null later
					
					return new RawRelationshipData( id, relData.firstNode(), 
						relData.secondNode(), relData.relationshipType() );
				}
				else if ( operation == PersistenceManager.LOAD_REL_PROPERTIES )
				{
					int id = (int) ( ( Relationship ) param ).getId();
					PropertyData propData[] = relConsumer.getProperties( id );
					RawPropertyData properties[] = 
						new RawPropertyData[ propData.length ];
					for ( int i = 0; i < propData.length; i++ )
					{
						properties[i] = new RawPropertyData( 
							propData[i].getId(), 
							propData[i].getKey(), 
							propData[i].getValue() );
					}
					return properties;
				}
				else if ( operation == PersistenceManager.LOAD_PROPERTY_VALUE )
				{
					int id = ( ( Integer ) param ).intValue();
					return propStore.getPropertyValue( id );
				}
				else
				{
					throw new PersistenceException( 
						"Unkown operation[" + operation + "]" );
				}
			}
			catch ( Exception t )
			{
				throw new PersistenceException( t );
			}
		}
		
		public void performUpdate( Event event, EventData eventData ) 
			throws PersistenceUpdateFailedException
		{
			Object data = eventData.getData();
			if ( data instanceof NodeOperationEventData )
			{
				performNodeUpdate( event, (NodeOperationEventData) data );
			}
			else if ( data instanceof RelationshipOperationEventData )
			{
				performRelationshipUpdate( event, 
					(RelationshipOperationEventData) data );
			}
			else if ( data instanceof RelationshipTypeOperationEventData )
			{
				performRelationshipTypeUpdate( event, 
					(RelationshipTypeOperationEventData) data );
			}
			else
			{
				throw new PersistenceUpdateFailedException( 
					"Unkown event/data " + "type:[" + event + "]/[" + 
					data + "]" );
			}
		}
		
		private void performNodeUpdate( Event event, 
			NodeOperationEventData data ) 
			throws PersistenceUpdateFailedException
		{
			try
			{
				if ( event == Event.NODE_ADD_PROPERTY )
				{
					int propertyId = propStore.nextId();
					nodeConsumer.addProperty( 
						data.getNodeId(), propertyId, 
						data.getPropertyKey(), 
						data.getProperty() ); 
					data.setNewPropertyId( propertyId );
				}
				else if ( event == Event.NODE_CHANGE_PROPERTY )
				{
					nodeConsumer.changeProperty( data.getNodeId(), 
						data.getPropertyId(), data.getProperty() );
				}
				else if ( event == Event.NODE_REMOVE_PROPERTY )
				{
					nodeConsumer.removeProperty( data.getNodeId(), 
						data.getPropertyId() );
				}
				else if ( event == Event.NODE_DELETE )
				{
					int nodeId = data.getNodeId();
					PropertyData[] propData = 
						nodeConsumer.getProperties( nodeId );
					// delete first so props are added in stray map
					nodeConsumer.deleteNode( nodeId );
					// backwards more efficient since it is a linked list
					for ( int i = propData.length - 1; i >= 0; i-- )
					{
						nodeConsumer.removeProperty( nodeId, 
							propData[i].getId() );
					}
				}
				else if ( event == Event.NODE_CREATE )
				{
					int nodeId = data.getNodeId();
					nodeConsumer.createNode( nodeId );
				}
				else
				{
					throw new PersistenceUpdateFailedException( 
						"Unkown event: " + event );
				}
			}
			catch ( Exception t )
			{
				throw new PersistenceUpdateFailedException( "Event[" + 
					event + "]", t );
			}
		}
		
		private void performRelationshipUpdate( Event event, 
			RelationshipOperationEventData data ) 
			throws PersistenceUpdateFailedException
		{
			try
			{
				if ( event == Event.RELATIONSHIP_ADD_PROPERTY )
				{
					int propertyId = propStore.nextId();
					relConsumer.addProperty( 
						data.getRelationshipId(), propertyId, 
						data.getPropertyKey(), 
						data.getProperty() );
					data.setNewPropertyId( propertyId ); 
				}
				else if ( event == Event.RELATIONSHIP_CHANGE_PROPERTY )
				{
					relConsumer.changeProperty( data.getRelationshipId(),  
						data.getPropertyId(), data.getProperty() );
				}
				else if ( event == Event.RELATIONSHIP_REMOVE_PROPERTY )
				{
					relConsumer.removeProperty( data.getRelationshipId(), 
						data.getPropertyId() );
				}
				else if ( event == Event.RELATIONSHIP_DELETE )
				{
					int relId = data.getRelationshipId();
					PropertyData[] propData = 
						relConsumer.getProperties( relId );
					// delete first so props are added in stray map
					relConsumer.deleteRelationship( relId );
					// backwards more efficient since it is a linked list
					for ( int i = propData.length - 1; i >= 0; i-- )
					{
						relConsumer.removeProperty( relId, 
							propData[i].getId() );
					}
				}
				else if ( event == Event.RELATIONSHIP_CREATE )
				{
					Integer nodeIds[] = data.getNodeIds();
					int firstNodeId = nodeIds[0].intValue(); 
					int secondNodeId = nodeIds[1].intValue();
					relConsumer.createRelationship( data.getRelationshipId(), 
						firstNodeId, secondNodeId, data.getTypeId() );
				}
				else
				{
					throw new PersistenceUpdateFailedException( 
						"Unkown event: " + event );
				}
			}
			catch ( Exception t )
			{
				throw new PersistenceUpdateFailedException( "Event[" + 
					event + "]", t );
			}
		}
	
		private void performRelationshipTypeUpdate( Event event, 
				RelationshipTypeOperationEventData data ) 
				throws PersistenceUpdateFailedException
		{
			try
			{
				if ( event == Event.RELATIONSHIPTYPE_CREATE )
				{
					relTypeConsumer.addRelationshipType( data.getId(), 
						data.getName() );
				}
				else
				{
					throw new PersistenceUpdateFailedException( 
						"Unkown event: " + event );
				}
			}
			catch ( IOException e )
			{
				throw new PersistenceUpdateFailedException( "Event[" + 
					event + "]", e );
			}
		}
	}	

	public String toString()
	{
		return "A Nio Neo Db persistence source to [" + 
			dataSourceName + "]";
	}

	public int nextId( Class clazz )
	{
		return xaDs.nextId( clazz );
	}
	
	// for recovery, returns a xa
	public XAResource getXaResource()
	{
		return this.xaDs.getXaConnection().getXaResource();
	}
	
	public void setDataSourceName( String dataSourceName )
	{
		this.dataSourceName = dataSourceName;
	}
	
	public String getDataSourceName()
	{
		return this.dataSourceName;
	}
}
