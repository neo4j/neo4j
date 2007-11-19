package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.util.ArrayMap;

class RelationshipTypeHolder
{
	private static final RelationshipTypeHolder holder = 
		new RelationshipTypeHolder();
	private static Logger log = 
		Logger.getLogger( RelationshipTypeHolder.class.getName() );
	
	private ArrayMap<String,Integer> relTypes = new ArrayMap<String,Integer>();
	private Map<Integer,String> relTranslation =
		new ConcurrentHashMap<Integer,String>();
	
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
	
	// temporary hack for b6 that will be changed to property index like 
	// implementation
	private static class RelTypeCreater extends Thread
	{
		private boolean success = false;
		private String name;
		private int id = -1;
		
		RelTypeCreater( String name )
		{
			super();
			this.name = name;
		}
		
		synchronized boolean succeded()
		{
			return success;
		}
		
		synchronized int getRelTypeId()
		{
			return id;
		}
		
		public synchronized void run()
		{
			Transaction tx = Transaction.begin();
			try
			{
				id = IdGenerator.getGenerator().nextId( 
					RelationshipType.class );
				EventManager em = EventManager.getManager();
				EventData eventData = new EventData( new RelTypeOpData( id, 
					name ) );
				if ( !em.generateProActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
					eventData ) )
				{
					throw new RuntimeException( 
						"Generate pro-active event failed." );
				}
				em.generateReActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
					eventData );
				tx.success();
				tx.finish();
				success = true;
				System.out.println( "Created relationship type " + name );
			} 
			finally
			{
				if ( !success )
				{
					try
					{
						tx.failure();
						tx.finish();
					}
					catch ( Throwable t ) 
					{ // ok
					}
				}
				this.notify();
			}
		}
	}

	private synchronized int createRelationshipType( String name )
	{
		Integer id = relTypes.get( name );
		if ( id != null )
		{
			return id;
		}
		RelTypeCreater createrThread = new RelTypeCreater( name );
		synchronized ( createrThread )
		{
			createrThread.start();
			while( createrThread.isAlive() )
			{
				try
                {
	                createrThread.wait( 50 );
                }
                catch ( InterruptedException e )
                { // ok
                }
			}
		}
		if ( createrThread.succeded() )
		{
			addRelType( name, createrThread.getRelTypeId() );
			return createrThread.getRelTypeId();
		}
		throw new RuntimeException( "Unable to create relationship type " + 
			name );
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
