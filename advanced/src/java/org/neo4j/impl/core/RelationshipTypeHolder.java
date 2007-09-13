package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionUtil;
import org.neo4j.impl.util.ArrayMap;

class RelationshipTypeHolder
{
	private static final RelationshipTypeHolder holder = 
		new RelationshipTypeHolder();
	private static Logger log = 
		Logger.getLogger( RelationshipTypeHolder.class.getName() );
	
	private ArrayMap<String,Integer> relTypes = new ArrayMap<String,Integer>();
	private Map<Integer,String> relTranslation =
		new HashMap<Integer,String>();
	
	private RelationshipTypeHolder()
	{
	}
	
	static RelationshipTypeHolder getHolder()
	{
		return holder;
	}
	
	void addRawRelationshipTypes( RawRelationshipTypeData[] types )
	{
		for ( int i = 0; i < types.length; i++ )
		{
			relTypes.put( types[i].getName(), types[i].getId() );
			relTranslation.put( types[i].getId(), types[i].getName() );
		}
	}

	public void addValidRelationshipTypes( 
		Class<? extends RelationshipType> relTypeClass )
	{
		for ( RelationshipType enumConstant : relTypeClass.getEnumConstants() )
		{
			String name = Enum.class.cast( enumConstant ).name();
			if ( relTypes.get( name ) == null )
			{
				int id = createRelationshipType( name );
				relTranslation.put( id, name );
			}
			else
			{
				relTranslation.put( relTypes.get( name ), name );
			}
		}
	}

	public RelationshipType addValidRelationshipType( String name, 
		boolean create ) 
	{
		if ( relTypes.get( name ) == null )
		{
			if ( !create )
			{
				return null;
			}
			int id = createRelationshipType( name );
			relTranslation.put( id, name );
		}
		else
		{
			relTranslation.put( relTypes.get( name ), name );
		}
		return new RelationshipTypeImpl( name );
	}
	
	boolean isValidRelationshipType( RelationshipType type )
	{
		return relTypes.get( type.name() ) != null;
	}
	
	RelationshipType getRelationshipTypeByName( String name )
	{
		if ( relTypes.get( name  ) != null )
		{
			return new RelationshipTypeImpl( name );
		}
		return null;
	}
	
	private static class RelationshipTypeImpl implements RelationshipType
	{
		private String name;
		
		RelationshipTypeImpl( String name )
		{
			assert name != null;
			this.name = name;
		}
		
		public String name()
		{
			return name;
		}
		
		public String toString()
		{
			return name;
		}
		
		public boolean equals( Object o )
		{
			if ( !(o instanceof RelationshipType) )
			{
				return false;
			}
			return name.equals( ((RelationshipType) o ).name() );
		}
		
		public int hashCode()
		{
			return name.hashCode();
		}
	}

	private int createRelationshipType( String name )
	{
		boolean txStarted = TransactionUtil.beginTx();
		boolean success = false;
		int id = IdGenerator.getGenerator().nextId( 
			RelationshipType.class );
		try
		{
			addRelType( name, id );
			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( new RelTypeOpData( id, 
				name ) );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
				eventData ) )
			{
				setRollbackOnly();
				throw new RuntimeException( 
					"Generate pro-active event failed." );
			}
			log.fine( "Created relationship type: " + name + "(" + id + ")" );
			em.generateReActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
				eventData );
			success = true;
			return id;
		}
		finally
		{
			TransactionUtil.finishTx( success, txStarted );
		}
	}

	private void setRollbackOnly()
	{
		try
		{
			TransactionFactory.getTransactionManager().setRollbackOnly();
		}
		catch ( javax.transaction.SystemException se )
		{
			se.printStackTrace();
			log.severe( "Failed to set transaction rollback only" );
		}
	}

	static class RelTypeOpData implements RelationshipTypeOperationEventData
	{
		private int id = -1;
		private String name = null;
		
		RelTypeOpData( int id, String name )
		{
			this.id = id;
			this.name = name;
		}
	
		public int getId()
		{
			return this.id;
		}
		
		public String getName()
		{
			return this.name;
		}
		
		public Object getEntity()
		{
			return this;
		}
	}
	
	void addRelType( String name, Integer id )
	{
		relTypes.put( name, id );
	}
	
	void removeRelType( String name )
	{
		relTypes.remove( name  );
	}

	void removeRelType( int id )
	{
		String name = relTranslation.remove( id );
		if ( name != null )
		{
			relTypes.remove( name  );
		}
	}
	
	int getIdFor( RelationshipType type )
	{
		return relTypes.get( type.name() );
	}
	
	RelationshipType getRelationshipType( int id )
	{
		String name = relTranslation.get( id );
		if ( name != null )
		{
			return new RelationshipTypeImpl( name );
		}
		return null;
	}

	public Iterable<RelationshipType> getRelationshipTypes()
    {
		List<RelationshipType> relTypeList = new ArrayList<RelationshipType>(); 
	    for ( String name : relTypes.keySet() )
	    {
	    	relTypeList.add( new RelationshipTypeImpl( name ) );
	    }
		return relTypeList;
    }

	public boolean hasRelationshipType( String name )
    {
		return relTypes.get( name ) != null;
    }
	
	void clear()
	{
		relTypes = new ArrayMap<String,Integer>();
		relTranslation = new HashMap<Integer,String>();
	}
}
