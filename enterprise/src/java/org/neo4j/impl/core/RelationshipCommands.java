package org.neo4j.impl.core;

import org.neo4j.api.core.RelationshipType;

import org.neo4j.impl.command.Command;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.command.UndoFailedException;
import org.neo4j.impl.persistence.PersistenceMetadata;

/**
 * This class has all the relationship commands encapsulated and also 
 * implements the {@link RelationshipOperationEventData} and 
 * {@link PersistenceMetadata} interfaces that is used by the the 
 * {@link org.neo4j.impl.persistence} package. The reason for having all 
 * this in the same class is to hold down the number of objects and make 
 * better use of object pooling. 
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
class RelationshipCommands extends Command implements 
	RelationshipOperationEventData,
	PersistenceMetadata
{
	private enum Type { 
		RESET, DELETE, ADD_PROPERTY, REMOVE_PROPERTY, CHANGE_PROPERTY, CREATE } 
	
	private Type type = Type.RESET;
	private RelationshipImpl relationship = null;
	
	private int propertyId = -1;
	// for add/remove/change property
	private PropertyIndex index = null;
	private Property value = null;
	// for remove/change property
	private Property oldProperty = null;
	
	private NodeImpl startNode;
	private NodeImpl endNode;
	
	protected RelationshipCommands()
	{
		super();
	}
	
	private synchronized void checkResetAndRelationship( Type newType )
	{
		assert relationship != null;
		assert newType != Type.RESET;
		type = newType;
		addCommandToTransaction();
	}
	
	void setStartNode( NodeImpl node )
	{
		this.startNode = node;
	}
	
	void setEndNode( NodeImpl node )
	{
		this.endNode = node;
	}
	
	void setRelationship( RelationshipImpl rel )
	{
		this.relationship = rel;
	}
	
	void initDelete()
	{
		checkResetAndRelationship( Type.DELETE );
	}
	
	private void executeDelete() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addRelationship( relationship );
			getTransactionCache().addNode( startNode );
			getTransactionCache().addNode( endNode );
			int relId = (int) relationship.getId();
			RelationshipType relType = relationship.getType();
			startNode.removeRelationship( relType, relId );
			endNode.removeRelationship( relType, relId );
			NodeManager.getManager().removeRelationshipFromCache( relId );
			relationship.setIsDeleted( true );
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
			int relId = (int) relationship.getId();
			RelationshipType relType = relationship.getType();
			startNode.addRelationship( relType, relId );
			endNode.addRelationship( relType, relId );
			relationship.setIsDeleted( false );
			NodeManager.getManager().addRelationshipToCache( relationship );
		}
		catch ( CreateException e )
		{
			throw new UndoFailedException( e );
		}
	}

	void initAddProperty( PropertyIndex index, Property value )
	{
		checkResetAndRelationship( Type.ADD_PROPERTY );
		this.index = index;
		this.value = value;
	}
	
	private void executeAddProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addRelationship( relationship );
			relationship.doAddProperty( index, value );
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
			relationship.doRemoveProperty( index );
		}
		catch ( NotFoundException e )
		{
			throw new UndoFailedException( e );
		}
	}

	void initRemoveProperty( int propertyId, PropertyIndex index )
	{
		checkResetAndRelationship( Type.REMOVE_PROPERTY );
		this.propertyId = propertyId; 
		this.index = index;
	}
	
	private void executeRemoveProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addRelationship( relationship );
			oldProperty = relationship.doRemoveProperty( index );
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
			relationship.doAddProperty( index, oldProperty );
		}
		catch ( IllegalValueException e )
		{
			throw new UndoFailedException( e );
		}
	}
	
	void initChangeProperty( int propertyId, PropertyIndex index, 
		Property newValue )
	{
		checkResetAndRelationship( Type.CHANGE_PROPERTY );
		this.propertyId = propertyId;
		this.index = index;
		this.value = newValue;
	}
	
	private void executeChangeProperty() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addRelationship( relationship );
			oldProperty = relationship.doChangeProperty( index, value );
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
			relationship.doChangeProperty( index, oldProperty );
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
		checkResetAndRelationship( Type.CREATE );
	}
	
	private void executeCreate() throws ExecuteFailedException
	{
		try
		{
			getTransactionCache().addNode( startNode );
			getTransactionCache().addNode( endNode );
			getTransactionCache().addRelationship( relationship );
			int relId = (int) relationship.getId();
			RelationshipType relType = relationship.getType();
			startNode.addRelationship( relType, relId );
			endNode.addRelationship( relType, relId );
			NodeManager.getManager().addRelationshipToCache( relationship );
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
			int relId = (int) relationship.getId();
			RelationshipType relType = relationship.getType();
			startNode.removeRelationship( relType, relId );
			endNode.removeRelationship( relType, relId );
			NodeManager.getManager().removeRelationshipFromCache( relId );
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
		relationship = null;
	}
	
	public int getRelationshipId()
	{
		return (int) this.relationship.getId();
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
	
	public Object getOldProperty()
	{
		return oldProperty.getValue();
	} 
	
	public Object getEntity()
	{
		return relationship;
	}
	
	public int getPropertyId()
	{
		assert type == Type.CHANGE_PROPERTY || type == Type.REMOVE_PROPERTY;
		return this.propertyId;
	}

	public void setNewPropertyId( int propertyId )
	{
		this.value.setId( propertyId );
		this.propertyId = propertyId; 
	}

	public int getStartNodeId()
	{
		return relationship.getStartNodeId();
	}
	
	public int getEndNodeId()
	{
		return relationship.getEndNodeId();
	}
	
	public RelationshipType getType()
	{
		return relationship.getType();
	}
	
	public int getTypeId()
	{
		return RelationshipTypeHolder.getHolder().getIdFor( 
			relationship.getType() );
	}
}