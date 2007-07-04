package org.neo4j.impl.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.command.Command;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceMetadata;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionUtil;

class RelationshipTypeHolder
{
	private static RelationshipTypeHolder holder = 
		new RelationshipTypeHolder();
	private static Logger log = 
		Logger.getLogger( RelationshipTypeHolder.class.getName() );
	
	private Set<Class<? extends RelationshipType>> enumClasses = 
		new HashSet<Class<? extends RelationshipType>>();
	private Map<String,Integer> relTypes = new HashMap<String,Integer>();
	private Map<Integer,RelationshipType> relTranslation =
		new HashMap<Integer,RelationshipType>();
	private Map<RelationshipType,String> validTypes = 
		new HashMap<RelationshipType,String>();
	
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
		}
	}

	public void addValidRelationshipTypes( 
		Class<? extends RelationshipType> relTypeClass )
	{
		enumClasses.add( relTypeClass );
		for ( RelationshipType enumConstant : relTypeClass.getEnumConstants() )
		{
			String name = Enum.class.cast( enumConstant ).name();
			if ( !relTypes.containsKey( name ) )
			{
				int id = createRelationshipType( name );
				relTranslation.put( id, enumConstant );
			}
			else
			{
				relTranslation.put( relTypes.get( name ), enumConstant );
			}
			validTypes.put( enumConstant, name );
		}
	}

	boolean isValidRelationshipType( RelationshipType type )
	{
		if ( type == null || !enumClasses.contains( type.getClass() ) )
			//type.getClass().equals( this.enumClass ) )
		{
			return false;
		}
		String name = Enum.class.cast( type ).name();
		return relTypes.containsKey( name );
		// .contains( name );
	}
	
	RelationshipType getRelationshipTypeByName( String name )
	{
		return relTranslation.get( relTypes.get( name ) );
	}

	private int createRelationshipType( String name )
	{
		boolean txStarted = TransactionUtil.beginTx();
		boolean success = false;
		int id = IdGenerator.getGenerator().nextId( 
			RelationshipType.class );
		CreateRelationshipTypeCommand command = 
			new CreateRelationshipTypeCommand();
		try
		{
			command.setId( id );
			command.setName( name );
			command.addToTransaction();
			command.execute();
			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( command );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
				eventData ) )
			{
				setRollbackOnly();
				command.undo();
				throw new RuntimeException( 
					"Generate pro-active event failed." );
			}
			log.fine( "Created relationship type: " + name + "(" + id + ")" );
			em.generateReActiveEvent( Event.RELATIONSHIPTYPE_CREATE, 
				eventData );
			success = true;
			return id;
		}
		catch ( ExecuteFailedException e )
		{
			command.undo();
			throw new RuntimeException( "Failed executing command.", e );
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

	private static class CreateRelationshipTypeCommand extends Command
		implements RelationshipTypeOperationEventData, PersistenceMetadata
	{
		private int id = -1;
		private String name = null;
		
		protected CreateRelationshipTypeCommand()
		{
			super();
		}
	
		public void addToTransaction()
		{
			addCommandToTransaction();
		}

		protected void onExecute()
		{
			RelationshipTypeHolder.getHolder().addRelType( 
				name, id );
		}
	
		protected void onUndo()
		{
			RelationshipTypeHolder.getHolder().removeRelType( name );
		}
	
		protected synchronized void onReset()
		{
			id = -1;
			name = null;
		}
		
		void  setId( int id )
		{
			this.id = id;
		}
		
		public int getId()
		{
			return this.id;
		}
		
		void setName( String name )
		{
			this.name = name;
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

	int getIdFor( RelationshipType type )
	{
		String name = Enum.class.cast( type ).name();
		return relTypes.get( name ); 
	}
	
	RelationshipType getRelationshipType( int id )
	{
		return relTranslation.get( id );
	}
}
