package org.neo4j.impl.core;

import org.neo4j.impl.command.Command;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.command.UndoFailedException;
import org.neo4j.impl.persistence.PersistenceMetadata;

class PropertyIndexCommands extends Command implements 
	PropertyIndexOperationEventData,
	PersistenceMetadata
{
	private enum Type { 
		RESET, CREATE, DELETE } 
	
	private Type type = Type.RESET;
	private PropertyIndex index = null;
	
	protected PropertyIndexCommands()
	{
		super();
	}
	
	private synchronized void checkResetAndPropertyIndex( Type newType )
	{
		assert index != null;
		assert newType != Type.RESET;
		type = newType;
		addCommandToTransaction();
	}
	
	void setPropertyIndex( PropertyIndex index )
	{
		this.index = index;
	}
	
	void initDelete()
	{
		checkResetAndPropertyIndex( Type.DELETE );
	}
	
	private void executeDelete() throws ExecuteFailedException
	{
		throw new ExecuteFailedException( "not implemented yet" );
	}
	
	private void undoDelete()
	{
		throw new UndoFailedException( "not implemented yet" );
	}

	void initCreate()
	{
		checkResetAndPropertyIndex( Type.CREATE );
	}
	
	private void executeCreate() throws ExecuteFailedException
	{
		PropertyIndex.addPropertyIndex( index );
	}	
	
	private void undoCreate()
	{
		PropertyIndex.removePropertyIndex( index );
	}

	protected void onExecute() throws ExecuteFailedException
	{
		switch ( type )
		{
			case RESET: 
				throw new RuntimeException( 
				"Unable to execute, command not initialized." );
			case DELETE: executeDelete(); break;
			case CREATE: executeCreate(); break;
			default:
				throw new RuntimeException( 
				"Unkown command type -> " + type );
		}
	}
	
	protected void onUndo()
	{
		switch ( type )
		{
			case RESET: 
				throw new RuntimeException( 
				"Unable to undo, command not initialized." );
			case DELETE: undoDelete(); break;
			case CREATE: undoCreate(); break;
			default:
				throw new RuntimeException( 
				"Unkown command type -> " + type );
		}
	}
	
	protected synchronized void onReset()
	{
		type = Type.RESET;
		index = null;
	}
	
	public PropertyIndex getIndex()
	{
		return index;
	} 
	
	public Object getEntity()
	{
		return index;
	}
}