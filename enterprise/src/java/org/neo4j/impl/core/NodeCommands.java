package org.neo4j.impl.core;

import org.neo4j.impl.command.Command;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.command.UndoFailedException;
import org.neo4j.impl.persistence.PersistenceMetadata;

/**
 * This class has all the node commands encapsulated and also implements
 * the {@link NodeOperationEventData} and {@link PersistenceMetadata} 
 * interfaces that is used by the the {@link org.neo4j.impl.persistence}
 * package. The reason for having all this in the same class is to hold down 
 * the number of objects and make better use of object pooling. 
 * <p>
 * Each operation has a initCommand method that will take all the data
 * that is needed to perform the operation and all data that is needed
 * by the persistence layer when making changes persistent. When the
 * command has been initialized it can be executed and undone via the
 * {@link org.neo4j.impl.command.Command command framework}.
 * <p>
 * A command can only be used once, keeping reference to an already 
 * initialized command is useless because a second init request on any
 * operation would result in an exception. Use the {@link NodeCommandFactory}
 * to create <CODE>NodeCommands</CODE> and you'll get pooling and reuse of 
 * objects automatically.
 */
class NodeCommands extends Command implements 
	NodeOperationEventData, PersistenceMetadata
{
	private enum Type { 
		RESET, DELETE, ADD_PROPERTY, REMOVE_PROPERTY, CHANGE_PROPERTY, CREATE } 
	
	private Type type = Type.RESET;
	private NodeImpl node = null;

	private int propertyId = -1;
	// for add/remove/change property
	private PropertyIndex index = null;
	private Property value = null;
	// for remove/change property
	private Property oldProperty = null;
	
	protected NodeCommands()
	{
		super();
	}
	
	// Makes sure this command is valid and sets the new command type
	private synchronized void checkResetAndNode( Type newType )
	{
		assert node != null;
		assert newType != Type.RESET;
		type = newType;
		addCommandToTransaction();
	}
	
	void setNode( NodeImpl node )
	{
		this.node = node;
	}
	
	void initDelete()
	{
		checkResetAndNode( Type.DELETE );
	}
	
	private void executeDelete() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( node );
			NodeManager.getManager().removeNodeFromCache( (int) node.getId() );
			node.setIsDeleted( true );
		}
		catch ( DeleteException e )
		{
			throw new ExecuteFailedException( e );
		}
	}
	
	private void undoDelete()
	{
		try
		{
			node.setIsDeleted( false );
			NodeManager.getManager().addNodeToCache( node );
		}
		catch ( CreateException e )
		{
			throw new UndoFailedException( e );
		}
	}
	
	void initAddProperty( PropertyIndex index, Property value )
	{
		checkResetAndNode( Type.ADD_PROPERTY );
		this.index = index;
		this.value = value;
	}
	
	private void executeAddProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( node );
			node.doAddProperty( index, value );
		}
		catch ( IllegalValueException e )
		{
			throw new ExecuteFailedException( e );
		}
	}	
	
	private void undoAddProperty()
	{
		try
		{
			node.doRemoveProperty( index );
		}
		catch ( NotFoundException e )
		{
			throw new UndoFailedException( e );
		}
	}

	void initRemoveProperty( int propertyId, PropertyIndex index )
	{
		checkResetAndNode( Type.REMOVE_PROPERTY );
		this.index = index;
		this.propertyId = propertyId;
	}
	
	private void executeRemoveProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( node );
			oldProperty = node.doRemoveProperty( index );
		}
		catch ( NotFoundException e )
		{
			throw new ExecuteFailedException( e );
		}
	}	
	
	private void undoRemoveProperty()
	{
		try
		{
			node.doAddProperty( index, oldProperty );
		}
		catch ( IllegalValueException e )
		{
			throw new UndoFailedException( e );
		}
	}
	
	void initChangeProperty( int propertyId, PropertyIndex index, 
		Property newValue )
		
	{
		checkResetAndNode( Type.CHANGE_PROPERTY );
		this.index = index;
		this.propertyId = propertyId;
		this.value = newValue;
	}
	
	private void executeChangeProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( node );
			oldProperty = node.doChangeProperty( index, value );
		}
		catch ( IllegalValueException e )
		{
			throw new ExecuteFailedException( e );
		}
		catch ( NotFoundException e )
		{
			throw new ExecuteFailedException( e );
		}
	}	
	
	private void undoChangeProperty()
	{
		try
		{
			node.doChangeProperty( index, oldProperty );
		}
		catch ( IllegalValueException e )
		{
			throw new UndoFailedException( e );
		}
		catch ( NotFoundException e )
		{
			throw new UndoFailedException( e );
		}
	}
	
	void initCreate()
	{
		checkResetAndNode( Type.CREATE );
	}
	
	private void executeCreate() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( node );
			NodeManager.getManager().addNodeToCache( node );
		}
		catch ( CreateException e )
		{
		 	throw new ExecuteFailedException( e );
		}
	}	
	
	private void undoCreate()
	{
		try
		{
			NodeManager.getManager().removeNodeFromCache( (int) node.getId() );
		}
		catch ( DeleteException e )
		{
			throw new UndoFailedException( e );
		}
	}
	

	protected void onExecute() throws ExecuteFailedException
	{
		switch ( type )
		{
			case RESET: 
				throw new RuntimeException( 
				"Unable to execute, command not initialized." );
			case DELETE: executeDelete(); break;
			case ADD_PROPERTY: executeAddProperty(); break;
			case REMOVE_PROPERTY: executeRemoveProperty(); break;
			case CHANGE_PROPERTY: executeChangeProperty(); break;
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
			case ADD_PROPERTY: undoAddProperty(); break;
			case REMOVE_PROPERTY: undoRemoveProperty(); break;
			case CHANGE_PROPERTY: undoChangeProperty(); break;
			case CREATE: undoCreate(); break;
			default:
				throw new RuntimeException( 
				"Unkown command type -> " + type );
		}
	}
	
	protected synchronized void onReset()
	{
		type = Type.RESET;
		node = null;
	}
	
	public Object getOldProperty()
	{
		return oldProperty.getValue();
	} 
	
	public int getNodeId()
	{
		return ( int ) node.getId();
	} 
	
	public int getPropertyId()
	{
		assert type == Type.CHANGE_PROPERTY || type == Type.REMOVE_PROPERTY;
		return propertyId;
	}
	
	public PropertyIndex getPropertyIndex()
	{
		assert type == Type.ADD_PROPERTY || type == Type.REMOVE_PROPERTY ||
			type == Type.CHANGE_PROPERTY;
		return index;
	}
	
	public Object getProperty()
	{
		assert type == Type.ADD_PROPERTY || type == Type.REMOVE_PROPERTY ||
			type == Type.CHANGE_PROPERTY;
		return value.getValue();
	}
	
	public Object getEntity()
	{
		return node;
	}
	
	public void setNewPropertyId( int propertyId )
	{
		value.setId( propertyId );
		this.propertyId = propertyId;
	}
}