package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.DynamicRecord;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeRecord;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.PropertyType;
import org.neo4j.impl.nioneo.store.ReadFromBuffer;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipRecord;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.impl.transaction.xaframework.XaTransaction;

/**
 * Transaction containing {@link MemCommand commads} reflecting the operations
 * performed in the transaction.
 */
class NeoTransaction extends XaTransaction
{
	private static Logger logger = 
		Logger.getLogger( NeoTransaction.class.getName() );
	
//	private Map<Integer,MemCommand> createdNodesMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> deletedNodesMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> createdRelsMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> deletedRelsMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> addedPropsMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> changedPropsMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> removedPropsMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> createdRelTypesMap = 
//		new HashMap<Integer,MemCommand>();
//	private Map<Integer,MemCommand> strayPropMap = 
//		new HashMap<Integer,MemCommand>();

//	private List<MemCommand.NodeCreate> createdNodesMap = 
//		new ArrayList<MemCommand.NodeCreate>();
//	private List<MemCommand.NodeDelete> deletedNodesMap = 
//		new ArrayList<MemCommand.NodeDelete>();
//	private List<MemCommand.RelationshipCreate> createdRelsMap = 
//		new ArrayList<MemCommand.RelationshipCreate>();
//	private List<MemCommand.RelationshipDelete> deletedRelsMap = 
//		new ArrayList<MemCommand.RelationshipDelete>();
	
//	private List<MemCommand> addedPropsMap = new ArrayList<MemCommand>();
//	private List<MemCommand> changedPropsMap = new ArrayList<MemCommand>();
//	private List<MemCommand> removedPropsMap = new ArrayList<MemCommand>();
//	private List<MemCommand.RelationshipTypeAdd> createdRelTypesMap = 
//		new ArrayList<MemCommand.RelationshipTypeAdd>();
//	private List<MemCommand> strayPropMap = new ArrayList<MemCommand>();
	
	private Map<Integer,NodeRecord> nodeRecords = 
		new HashMap<Integer,NodeRecord>();
	private Map<Integer,PropertyRecord> propertyRecords = 
		new HashMap<Integer,PropertyRecord>();
	private Map<Integer,RelationshipRecord> relRecords = 
		new HashMap<Integer,RelationshipRecord>();
	private Map<Integer,RelationshipTypeRecord> relTypeRecords =
		new HashMap<Integer,RelationshipTypeRecord>();
	private Map<Integer,PropertyIndexRecord> propIndexRecords =
		new HashMap<Integer,PropertyIndexRecord>();
	
	private ArrayList<Command.NodeCommand> nodeCommands = 
		new ArrayList<Command.NodeCommand>();
	private ArrayList<Command.PropertyCommand> propCommands = 
		new ArrayList<Command.PropertyCommand>();
	private ArrayList<Command.PropertyIndexCommand> propIndexCommands = 
		new ArrayList<Command.PropertyIndexCommand>();
	private ArrayList<Command.RelationshipCommand> relCommands = 
		new ArrayList<Command.RelationshipCommand>();
	private ArrayList<Command.RelationshipTypeCommand> relTypeCommands = 
		new ArrayList<Command.RelationshipTypeCommand>();
	
	private NeoStore neoStore;
	private ReadFromBuffer readFromBuffer;
	private boolean committed = false;
	private boolean prepared = false;
	
	NeoTransaction( int identifier, XaLogicalLog log, NeoStore neoStore )
	{
		super( identifier, log );
		this.neoStore = neoStore;
		this.readFromBuffer = neoStore.getNewReadFromBuffer();
	}
	
	public boolean isReadOnly()
	{
		if ( isRecovered() )
		{
			if ( nodeCommands.size() == 0 && propCommands.size() == 0 && 
				relCommands.size() == 0 && relTypeCommands.size() == 0 &&
				propIndexCommands.size() == 0 )
			{
				return true;
			}
			return false;
		}
//		if ( createdNodesMap.size() == 0 && deletedNodesMap.size() == 0 &&
//			createdRelsMap.size() == 0 && deletedRelsMap.size() == 0 &&
//			addedPropsMap.size() == 0 && changedPropsMap.size() == 0 &&
//			removedPropsMap.size() == 0 && createdRelTypesMap.size() == 0 )
		if ( nodeRecords.size() == 0 && relRecords.size() == 0 && 
			relTypeRecords.size() == 0 && propertyRecords.size() == 0 )
		{
			return true;
		}
		return false;
	}
	
	public void doAddCommand( XaCommand command )
	{
		// do nothing, commands are created and added in prepare
	}
	
//	public void addmemorycommand( MemCommand command ) throws IOException
//	{
//		assert !committed;
//		assert !prepared;
//		int key = command.getId();
//		if ( command instanceof MemCommand.NodeCreate )
//		{
//			// createdNodesMap.put( key, command );
//			createdNodesMap.add( (MemCommand.NodeCreate) command );
//		}
//		else if ( command instanceof MemCommand.RelationshipCreate )
//		{
//			// createdRelsMap.put( key, command );
//			createdRelsMap.add( (MemCommand.RelationshipCreate) command );
//		}
//		else if ( command instanceof MemCommand.NodeDelete )
//		{
////			deletedNodesMap.put( key, command );
//			deletedNodesMap.add( (MemCommand.NodeDelete) command );
//		}
//		else if ( command instanceof MemCommand.RelationshipDelete )
//		{
////			deletedRelsMap.put( key, command );
//			deletedRelsMap.add( (MemCommand.RelationshipDelete) command );
//		}
//		else if ( command instanceof MemCommand.NodeAddProperty )
//		{
////			addedPropsMap.put( key, command );
//			addedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.NodeChangeProperty )
//		{
////			
////			if ( addedPropsMap.containsKey( key ) )
////			{
////				MemCommand.NodeAddProperty propCommand = 
////					( MemCommand.NodeAddProperty ) addedPropsMap.get( key );
////				propCommand.setNewValue( ( ( MemCommand.NodeChangeProperty ) 
////					command ).getValue() );
////			}
////			else if ( changedPropsMap.containsKey( key ) )
////			{
////				MemCommand.NodeChangeProperty propCommand = 
////					( MemCommand.NodeChangeProperty ) changedPropsMap.get( 
////						key );
////				propCommand.setNewValue( ( ( MemCommand.NodeChangeProperty ) 
////					command ).getValue() );
////			}
////			else
////			{
////				changedPropsMap.put( key, command );
////			}
//			changedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.NodeRemoveProperty )
//		{
////			int nodeId = ( ( MemCommand.NodeRemoveProperty ) 
////				command ).getNodeId();
////
////			if ( addedPropsMap.containsKey( key ) || 
////				changedPropsMap.containsKey( key ) )
////			{
////				MemCommand cmd = addedPropsMap.remove( key );
////				if ( cmd != null )
////				{
////					getPropertyStore().freeId( cmd.getId() );
////				}
////				cmd = changedPropsMap.remove( key );
////			}
////			else if ( !deletedNodesMap.containsKey( nodeId ) ) 
////			{
////				removedPropsMap.put( key, command );
////			}
////			else
////			{
////				strayPropMap.put( key, command );
////			}
//			removedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.RelationshipAddProperty )
//		{
////			addedPropsMap.put( key, command );
//			addedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.RelationshipChangeProperty )
//		{
////			if ( addedPropsMap.containsKey( key ) )
////			{
////				MemCommand.RelationshipAddProperty propCommand = 
////					( MemCommand.RelationshipAddProperty ) 
////						addedPropsMap.get( key );
////				propCommand.setNewValue( 
////					( ( MemCommand.RelationshipChangeProperty ) 
////						command ).getValue() );
////			}
////			else if ( changedPropsMap.containsKey( key ) )
////			{
////				MemCommand.RelationshipChangeProperty propCommand = 
////					( MemCommand.RelationshipChangeProperty ) 
////						changedPropsMap.get( key );
////				propCommand.setNewValue( 
////					( ( MemCommand.RelationshipChangeProperty ) 
////						command ).getValue() );
////			}
////			else
////			{
////				changedPropsMap.put( key, command );
////			}
//			changedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.RelationshipRemoveProperty )
//		{
////			int relId = ( ( MemCommand.RelationshipRemoveProperty ) 
////				command ).getRelId();
////			if ( addedPropsMap.containsKey( key ) || 
////				changedPropsMap.containsKey( key ) )
////			{
////				MemCommand cmd = addedPropsMap.remove( key );
////				if ( cmd != null )
////				{
////					getPropertyStore().freeId( cmd.getId() );
////				}
////				cmd = changedPropsMap.remove( key );
////			}
////			else if ( !deletedRelsMap.containsKey( relId ) ) 
////			{
////				removedPropsMap.put( key, command );
////			}
////			else
////			{
////				strayPropMap.put( key, command );
////			}
//			removedPropsMap.add( command );
//		}
//		else if ( command instanceof MemCommand.RelationshipTypeAdd )
//		{
////			createdRelTypesMap.put( key, command );
//			createdRelTypesMap.add( (MemCommand.RelationshipTypeAdd) command );
//		}
//		else
//		{
//			throw new RuntimeException( "Unkown command " + command );
//		}
//	}
	
	@Override
	protected void doPrepare() throws XAException
	{
		if ( committed )
		{
			throw new XAException( "Cannot prepare committed transaction[" + 
				getIdentifier() + "]" );
		}
		if ( prepared )
		{
			throw new XAException( "Cannot prepare prepared transaction[" + 
				getIdentifier() + "]" );
		}
		// generate records then write to logical log via addCommand method
		prepared = true;
//		try
//		{
////			Iterator<MemCommand> itr = 
////				createdRelTypesMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand.RelationshipTypeAdd command = 
////					(MemCommand.RelationshipTypeAdd) itr.next();
////				relationshipTypeAdd( command.getId(), command.getName() );
////			}
//			for ( MemCommand.RelationshipTypeAdd command : createdRelTypesMap )
//			{
//				relationshipTypeAdd( command.getId(), command.getName() );
//			}
//				
////			itr = createdNodesMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand.NodeCreate command = 
////					( MemCommand.NodeCreate ) itr.next();
////				if ( !deletedNodesMap.containsKey( command.getId() ) )
////				{
////					nodeCreate( command.getId() );
////				}
////				else
////				{
////					getNodeStore().freeId( command.getId() );
////				}
////			}
//			for ( MemCommand.NodeCreate command : createdNodesMap )
//			{
//				nodeCreate( command.getId() );
//			}
//			
////			itr = createdRelsMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand.RelationshipCreate command = 
////					( MemCommand.RelationshipCreate ) itr.next();
////				if ( !deletedRelsMap.containsKey( command.getId() ) )
////				{
////					relationshipCreate( command.getId(), command.getFirstNode(), 
////						command.getSecondNode(), command.getType() );
////				}
////				else
////				{
////					getRelationshipStore().freeId( command.getId() );
////				}
////			}
//			for ( MemCommand.RelationshipCreate command : createdRelsMap )
//			{
//				relationshipCreate( command.getId(), command.getFirstNode(), 
//					command.getSecondNode(), command.getType() );
//			}
//		
//			// property add,change,remove
//			Iterator<MemCommand> itr = addedPropsMap.iterator();
//			while ( itr.hasNext() )
//			{
//				MemCommand cmd = itr.next();
//				if ( cmd instanceof MemCommand.NodeAddProperty )
//				{
//					MemCommand.NodeAddProperty command = 
//						( MemCommand.NodeAddProperty ) cmd;
////					if ( !deletedNodesMap.containsKey( command.getNodeId() ) )
////					{
//						nodeAddProperty( command.getNodeId(), 
//							command.getPropertyId(), command.getKey(), 
//							command.getValue() );
////					}
//				}
//				else
//				{
//					MemCommand.RelationshipAddProperty command = 
//						( MemCommand.RelationshipAddProperty ) cmd;
////					if ( !deletedRelsMap.containsKey( command.getRelId() ) )
////					{
//						relAddProperty( command.getRelId(), 
//							command.getPropertyId(), command.getKey(), 
//							command.getValue() );
////					}
//				}
//			}
//			itr = changedPropsMap.iterator();
//			while ( itr.hasNext() )
//			{
//				MemCommand cmd = itr.next();
//				if ( cmd instanceof MemCommand.NodeChangeProperty )
//				{
//					MemCommand.NodeChangeProperty command = 
//						( MemCommand.NodeChangeProperty ) cmd;
////					if ( !deletedNodesMap.containsKey( command.getNodeId() ) )
////					{
//						nodeChangeProperty( command.getPropertyId(), 
//							command.getValue() );
////					}
//				}
//				else
//				{
//					MemCommand.RelationshipChangeProperty command = 
//						( MemCommand.RelationshipChangeProperty ) cmd;
////					if ( !deletedRelsMap.containsKey( command.getRelId() ) )
////					{
//						relChangeProperty( command.getPropertyId(), 
//							command.getValue() );
////					}
//				}
//			}
//			itr = removedPropsMap.iterator();
//			while ( itr.hasNext() )
//			{
//				MemCommand cmd = itr.next();
//				if ( cmd instanceof MemCommand.NodeRemoveProperty )
//				{
//					MemCommand.NodeRemoveProperty command = 
//						( MemCommand.NodeRemoveProperty ) cmd;
////					if ( !deletedNodesMap.containsKey( command.getNodeId() ) )
////					{
//						nodeRemoveProperty( command.getNodeId(), 
//							command.getPropertyId() );
////					}
//				}
//				else
//				{
//					MemCommand.RelationshipRemoveProperty command = 
//						( MemCommand.RelationshipRemoveProperty ) cmd;
////					if ( !deletedRelsMap.containsKey( command.getRelId() ) )
////					{
//						relRemoveProperty( command.getRelId(), 
//							command.getPropertyId() );
////					}
//				}
//			}
//	
////			itr = strayPropMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand cmd = itr.next();
////				if ( cmd instanceof MemCommand.NodeRemoveProperty )
////				{
////					MemCommand.NodeRemoveProperty command = 
////						( MemCommand.NodeRemoveProperty ) cmd;
////					if ( !deletedNodesMap.containsKey( command.getNodeId() ) )
////					{
////						nodeRemoveProperty( command.getNodeId(), 
////							command.getPropertyId() );
////					}
////				}
////				else
////				{
////					MemCommand.RelationshipRemoveProperty command = 
////						( MemCommand.RelationshipRemoveProperty ) cmd;
////					if ( !deletedRelsMap.containsKey( command.getRelId() ) )
////					{
////						relRemoveProperty( command.getRelId(), 
////							command.getPropertyId() );
////					}
////				}
////			}
//			
////			itr = deletedRelsMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand.RelationshipDelete command = 
////					(MemCommand.RelationshipDelete) itr.next();
////				if ( !createdRelsMap.containsKey( command.getId() ) )
////				{
////					relDelete( command.getId() );
////				}
////			}
//			for ( MemCommand.RelationshipDelete command : deletedRelsMap )
//			{
//				relDelete( command.getId() );
//			}
//			
////			itr = deletedNodesMap.values().iterator();
////			while ( itr.hasNext() )
////			{
////				MemCommand.NodeDelete command = 
////					(MemCommand.NodeDelete) itr.next();
////				if ( !createdNodesMap.containsKey( command.getId() ) )
////				{
////					nodeDelete( command.getId() );
////				}
////			}
//			for ( MemCommand.NodeDelete command : deletedNodesMap )
//			{
//				nodeDelete( command.getId() );
//			}
			
			for ( RelationshipTypeRecord record : relTypeRecords.values() )
			{
				Command.RelationshipTypeCommand command = 
					new Command.RelationshipTypeCommand( 
						neoStore.getRelationshipTypeStore(), record );
				relTypeCommands.add( command );
				addCommand( command );
			}
			for ( NodeRecord record : nodeRecords.values() )
			{
				Command.NodeCommand command = new Command.NodeCommand( 
					neoStore.getNodeStore(), record );
				nodeCommands.add( command );
				addCommand( command );
			}
			for ( RelationshipRecord record : relRecords.values() )
			{
				Command.RelationshipCommand command = 
					new Command.RelationshipCommand( 
						neoStore.getRelationshipStore(), record );
				relCommands.add( command );
				addCommand( command );
			}
			for ( PropertyIndexRecord record : propIndexRecords.values() )
			{
				Command.PropertyIndexCommand command = 
					new Command.PropertyIndexCommand( 
						neoStore.getPropertyStore().getIndexStore(), record );
				propIndexCommands.add( command );
				addCommand( command );
			}
			for ( PropertyRecord record : propertyRecords.values() )
			{
				Command.PropertyCommand command = new Command.PropertyCommand( 
					neoStore.getPropertyStore(), record );
				propCommands.add( command );
				addCommand( command );
			}
//		}
//		catch ( IOException e )
//		{
//			logger.log( Level.SEVERE, "Unable to prepare[" + getIdentifier() + 
//				"]", e );
//			throw new XAException( "" + e );
//		}
	}
	
	protected void injectCommand( XaCommand xaCommand ) 
	{
		if ( xaCommand instanceof Command.NodeCommand )
		{
			nodeCommands.add( ( Command.NodeCommand ) xaCommand );
		}
		else if ( xaCommand instanceof Command.RelationshipCommand )
		{
			relCommands.add( ( Command.RelationshipCommand ) xaCommand );
		}
		else if ( xaCommand instanceof Command.PropertyCommand )
		{
			propCommands.add( ( Command.PropertyCommand ) xaCommand );
		}
		else if ( xaCommand instanceof Command.PropertyIndexCommand )
		{
			propIndexCommands.add( ( Command.PropertyIndexCommand ) xaCommand );
		}
		else if ( xaCommand instanceof Command.RelationshipTypeCommand )
		{
			relTypeCommands.add( 
				( Command.RelationshipTypeCommand ) xaCommand );
		}
		else
		{
			throw new RuntimeException( "Unkown command " + xaCommand );
		}
	}

	public void doRollback() throws XAException
	{
		if ( committed )
		{
			throw new XAException( "Cannot rollback partialy commited " + 
				"transaction[" + getIdentifier() + "]. Recover and " + 
				"commit" );
		}
		try
		{
			for ( RelationshipTypeRecord record : relTypeRecords.values() )
			{
				if ( record.isCreated() )
				{
					getRelationshipTypeStore().freeId( record.getId() );
					for ( DynamicRecord dynamicRecord: record.getTypeRecords() )
					{
						if ( dynamicRecord.isCreated() )
						{
							getRelationshipTypeStore().freeBlockId( 
								dynamicRecord.getId() );
						}
					}
				}
			}
			for ( NodeRecord record : nodeRecords.values() )
			{
				if ( record.isCreated() )
				{
					getNodeStore().freeId( record.getId() );
				}
			}
			for ( RelationshipRecord record : relRecords.values() )
			{
				if ( record.isCreated() )
				{
					getRelationshipStore().freeId( record.getId() );
				}
			}
			for ( PropertyIndexRecord record : propIndexRecords.values() )
			{
				if ( record.isCreated() )
				{
					getPropertyStore().getIndexStore().freeId( record.getId() );
					for ( DynamicRecord dynamicRecord: record.getKeyRecords() )
					{
						if ( dynamicRecord.isCreated() )
						{
							getPropertyStore().getIndexStore().freeBlockId( 
								dynamicRecord.getId() );
						}
					}
				}
			}
			for ( PropertyRecord record : propertyRecords.values() )
			{
				if ( record.isCreated() )
				{
					getPropertyStore().freeId( record.getId() );
//					for ( DynamicRecord dynamicRecord: 
//						record.getValueRecords() )
//					{
//						// this doesn't work yet with change of property value
//						// and changing type
//						if ( dynamicRecord.isCreated() )
//						{
//							if ( record.getType() == PropertyType.STRING )
//							{
//								getPropertyStore().freeStringId( 
//									dynamicRecord.getId() );
//							}
//							else if ( record.getType() == PropertyType.ARRAY )
//							{
//								getPropertyStore().freeArrayId( 
//									dynamicRecord.getId() );
//							}
//							else
//							{
//								....`
//							}
//						}
//					}
				}
			}
		}
		catch ( IOException e )
		{
			logger.log( Level.SEVERE, "Unable to rollback", e ); 
			throw new XAException( "Unable to rollback transaction[" + 
				getIdentifier() + "], " + e);
		}
		
//		if ( !prepared )
//		{
//			try
//			{
////				Iterator<MemCommand> itr = 
////					createdRelTypesMap.values().iterator();
////				while ( itr.hasNext() )
////				{
////					MemCommand.RelationshipTypeAdd command = 
////						(MemCommand.RelationshipTypeAdd) itr.next();
////					getRelationshipTypeStore().freeId( command.getId() );
////				}
//				for ( MemCommand.RelationshipTypeAdd command : createdRelTypesMap )
//				{
//					getRelationshipTypeStore().freeId( command.getId() );
//				}
//					
////				itr = createdNodesMap.values().iterator();
////				while ( itr.hasNext() )
////				{
////					MemCommand.NodeCreate command = 
////						( MemCommand.NodeCreate ) itr.next();
////					if ( !deletedNodesMap.containsKey( command.getId() ) )
////					{
////						getNodeStore().freeId( command.getId() );
////					}
////				}
//				for ( MemCommand.NodeCreate command : createdNodesMap )
//				{
//					getNodeStore().freeId( command.getId() );
//				}
//				
////				itr = createdRelsMap.values().iterator();
////				while ( itr.hasNext() )
////				{
////					MemCommand.RelationshipCreate command = 
////						( MemCommand.RelationshipCreate ) itr.next();
////					if ( !deletedRelsMap.containsKey( command.getId() ) )
////					{
////						getRelationshipStore().freeId( command.getId() );
////					}
////				}
//				for ( MemCommand.RelationshipCreate command : createdRelsMap )
//				{
//					getRelationshipStore().freeId( command.getId() );
//				}
//				
//				// property add
//				Iterator<MemCommand> itr = addedPropsMap.iterator();
//				while ( itr.hasNext() )
//				{
//					MemCommand cmd = itr.next();
//					if ( cmd instanceof MemCommand.NodeAddProperty )
//					{
//						MemCommand.NodeAddProperty command = 
//							( MemCommand.NodeAddProperty ) cmd;
////						if ( !deletedNodesMap.containsKey( command.getNodeId() ) )
////						{
//							getPropertyStore().freeId( 
//								command.getPropertyId() );
////						}
//					}
//					else
//					{
//						MemCommand.RelationshipAddProperty command = 
//							( MemCommand.RelationshipAddProperty ) cmd;
////						if ( !deletedRelsMap.containsKey( command.getRelId() ) )
////						{
//							getPropertyStore().freeId( 
//								command.getPropertyId() );
////						}
//					}
//				}
//			}
//			catch ( IOException e )
//			{
//				logger.log( Level.SEVERE, "Unable to rollback", e ); 
//				throw new XAException( "Unable to rollback transaction[" + 
//					getIdentifier() + "], " + e);
//			}
//		}
	}
	
	public void doCommit() throws XAException
	{
		if ( !isRecovered() && !prepared )
		{
			throw new XAException( "Cannot commit non prepared transaction[" + 
				getIdentifier() + "]" );
		}
		TxInfoManager.getManager().registerMode( isRecovered() );
		try
		{
			committed = true;
			CommandSorter sorter = new CommandSorter();
			// reltypes
			java.util.Collections.sort( relTypeCommands, sorter );
			for ( Command.RelationshipTypeCommand command : relTypeCommands )
			{
				command.execute();
			}
			// nodes
			java.util.Collections.sort( nodeCommands, sorter );
			for ( Command.NodeCommand command : nodeCommands )
			{
				command.execute();
			}
			// relationships
			java.util.Collections.sort( relCommands, sorter );
			for ( Command.RelationshipCommand command : relCommands )
			{
				command.execute();
			}
			java.util.Collections.sort( propIndexCommands, sorter );
			for ( Command.PropertyIndexCommand command : propIndexCommands )
			{
				command.execute();
			}
			// properties
			java.util.Collections.sort( propCommands, sorter );
			for ( Command.PropertyCommand command : propCommands )
			{
				command.execute();
			}
		}
		catch ( Throwable t )
		{
			logger.log( Level.SEVERE, "Unable to commit tx[" + 
				getIdentifier() + "]", t );
			throw new XAException( "Unable to commit" + t );
		}
		finally
		{
			TxInfoManager.getManager().unregisterMode();
		}
	}
	
	private RelationshipTypeStore getRelationshipTypeStore()
	{
		return neoStore.getRelationshipTypeStore();
	}
	
	private NodeStore getNodeStore()
	{
		return neoStore.getNodeStore();
	}
	
	private RelationshipStore getRelationshipStore()
	{
		return neoStore.getRelationshipStore();
	}
	
	private PropertyStore getPropertyStore()
	{
		return neoStore.getPropertyStore();
	}
	
	public boolean nodeLoadLight( int nodeId ) throws IOException
    {
	    NodeRecord nodeRecord = getNodeRecord( nodeId );
	    if ( nodeRecord != null )
	    {
	    	return nodeRecord.inUse();
	    }
		return getNodeStore().loadLightNode( nodeId, readFromBuffer );
    }

	public RelationshipData relationshipLoad( int id ) throws IOException
    {
	    RelationshipRecord relRecord = getRelationshipRecord( id );
	    if ( relRecord != null )
	    {
	    	if ( !relRecord.inUse() )
	    	{
	    		throw new IOException( "relationship " + id + " not in use" );
	    	}
	    	return new RelationshipData( id, relRecord.getFirstNode(), 
	    		relRecord.getSecondNode(), relRecord.getType() );
	    }
	    relRecord = getRelationshipStore().getRecord( id, readFromBuffer );
    	return new RelationshipData( id, relRecord.getFirstNode(), 
    		relRecord.getSecondNode(), relRecord.getType() );
    }
	
	void nodeDelete( int nodeId ) throws IOException
	{
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
			addNodeRecord( nodeRecord );
		}
		nodeRecord.setInUse( false );
		int nextProp = nodeRecord.getNextProp();
		while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord propRecord = getPropertyRecord( nextProp );
			if ( propRecord == null )
			{
				propRecord = getPropertyStore().getRecord( nextProp, 
					readFromBuffer );
				addPropertyRecord( propRecord );
			}
			if ( propRecord.isLight() )
			{
				getPropertyStore().makeHeavy( propRecord, readFromBuffer );
			}
			
			nextProp = propRecord.getNextProp();
			propRecord.setInUse( false );
			// TODO: update count on property index record
//			for ( DynamicRecord keyRecord : propRecord.getKeyRecords() )
//			{
//				keyRecord.setInUse( false );
//			}
			for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
			{
				valueRecord.setInUse( false );
			}
		}
	}

	void relDelete( int id ) throws IOException
	{
		RelationshipRecord record = getRelationshipRecord( id );
		if ( record == null )
		{
			record = getRelationshipStore().getRecord( id, readFromBuffer );
			addRelationshipRecord( record );
		}
		int nextProp = record.getNextProp();
		while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord propRecord = getPropertyRecord( nextProp );
			if ( propRecord == null )
			{
				propRecord = getPropertyStore().getRecord( nextProp, 
					readFromBuffer );
				addPropertyRecord( propRecord );
			}
			if ( propRecord.isLight() )
			{
				getPropertyStore().makeHeavy( propRecord, readFromBuffer );
			}
			nextProp = propRecord.getNextProp();
			propRecord.setInUse( false );
			// TODO: update count on property index record
//			for ( DynamicRecord keyRecord : propRecord.getKeyRecords() )
//			{
//				keyRecord.setInUse( false );
//			}
			for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
			{
				valueRecord.setInUse( false );
			}
		}
		disconnectRelationship( record );
		updateNodes( record );
		record.setInUse( false );
	}
	
	private void disconnectRelationship( RelationshipRecord rel )
		throws IOException
	{
		// update first node prev
		if ( rel.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord prevRel = getRelationshipRecord( 
				rel.getFirstPrevRel() );
			if ( prevRel == null )
			{
				prevRel = getRelationshipStore().getRecord( 
					rel.getFirstPrevRel(), readFromBuffer );
				addRelationshipRecord( prevRel );
			}
			if ( prevRel.getFirstNode() == rel.getFirstNode() )
			{
				prevRel.setFirstNextRel( rel.getFirstNextRel() );
			}
			else if ( prevRel.getSecondNode() == rel.getFirstNode() )
			{
				prevRel.setSecondNextRel( rel.getFirstNextRel() );
			}
			else
			{
				throw new RuntimeException( prevRel + 
					" don't match " + rel );
			}
		}
		// update first node next
		if ( rel.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord nextRel = getRelationshipRecord( 
				rel.getFirstNextRel() );
			if ( nextRel == null )
			{
				nextRel = getRelationshipStore().getRecord( 
					rel.getFirstNextRel(), readFromBuffer );
				addRelationshipRecord( nextRel );
			}
			if ( nextRel.getFirstNode() == rel.getFirstNode() )
			{
				nextRel.setFirstPrevRel( rel.getFirstPrevRel() );
			}
			else if ( nextRel.getSecondNode() == rel.getFirstNode() )
			{
				nextRel.setSecondPrevRel( rel.getFirstPrevRel() );
			}
			else
			{
				throw new RuntimeException( nextRel + 
					" don't match " + rel );
			}
		}
		// update second node prev
		if ( rel.getSecondPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord prevRel = getRelationshipRecord( 
				rel.getSecondPrevRel() );
			if ( prevRel == null )
			{
				prevRel = getRelationshipStore().getRecord( 
					rel.getSecondPrevRel(), readFromBuffer );
				addRelationshipRecord( prevRel );
			}
			if ( prevRel.getFirstNode() == rel.getSecondNode() )
			{
				prevRel.setFirstNextRel( rel.getSecondNextRel() );
			}
			else if ( prevRel.getSecondNode() == rel.getSecondNode() )
			{
				prevRel.setSecondNextRel( rel.getSecondNextRel() );
			}
			else
			{
				throw new RuntimeException( prevRel + 
					" don't match " + rel );
			}
		}
		// update second node next
		if ( rel.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord nextRel = getRelationshipRecord( 
				rel.getSecondNextRel() );
			if ( nextRel == null )
			{
				nextRel = getRelationshipStore().getRecord( 
					rel.getSecondNextRel(), readFromBuffer );
				addRelationshipRecord( nextRel );
			}
			if ( nextRel.getFirstNode() == rel.getSecondNode() )
			{
				nextRel.setFirstPrevRel( rel.getSecondPrevRel() );
			}
			else if ( nextRel.getSecondNode() == rel.getSecondNode() )
			{
				nextRel.setSecondPrevRel( rel.getSecondPrevRel() );
			}
			else
			{
				throw new RuntimeException( nextRel + 
					" don't match " + rel );
			}
		}
	}
	
	public RelationshipData[] nodeGetRelationships( int nodeId )
		throws IOException
    {
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
		}
		int nextRel = nodeRecord.getNextRel();
		List<RelationshipData> rels = new ArrayList<RelationshipData>();
		while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord relRecord = getRelationshipRecord( nextRel );
			if ( relRecord == null )
			{
				relRecord = getRelationshipStore().getRecord( nextRel, 
					readFromBuffer );
			}
			int firstNode = relRecord.getFirstNode();
			int secondNode = relRecord.getSecondNode();
			rels.add( new RelationshipData( nextRel, firstNode, secondNode, 
				relRecord.getType() ) );
			if ( firstNode == nodeId )
			{
				nextRel = relRecord.getFirstNextRel();
			}
			else if ( secondNode == nodeId )
			{
				nextRel = relRecord.getSecondNextRel();
			}
			else
			{
				throw new RuntimeException( "GAH" );
			}
		}
		return rels.toArray( new RelationshipData[rels.size()] );
    }
	
	private void updateNodes( RelationshipRecord rel ) throws IOException
	{
		if ( rel.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
		{
			NodeRecord firstNode = getNodeRecord( rel.getFirstNode() );
			if ( firstNode == null )
			{
				firstNode = getNodeStore().getRecord( rel.getFirstNode(), 
					readFromBuffer );
				addNodeRecord( firstNode );
			}
			firstNode.setNextRel( rel.getFirstNextRel() );
		}
		if ( rel.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() ) 
		{
			NodeRecord secondNode = getNodeRecord( rel.getSecondNode() );
			if ( secondNode == null )
			{
				secondNode = getNodeStore().getRecord( rel.getSecondNode(), 
					readFromBuffer );
				addNodeRecord( secondNode );
			}
			secondNode.setNextRel( rel.getSecondNextRel() );
		}
	}
	
	void relRemoveProperty( int relId, int propertyId ) throws IOException
	{
		RelationshipRecord relRecord = getRelationshipRecord( relId );
		if ( relRecord == null )
		{
			relRecord = getRelationshipStore().getRecord( relId, 
				readFromBuffer );
		}
		PropertyRecord propRecord = getPropertyRecord( propertyId );
		if ( propRecord == null )
		{
			propRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propRecord );
		}
		if ( propRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propRecord, readFromBuffer );
		}
		
		propRecord.setInUse( false );
		// TODO: update count on property index record
//		for ( DynamicRecord keyRecord : propRecord.getKeyRecords() )
//		{
//			keyRecord.setInUse( false );
//		}
		for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
		{
			valueRecord.setInUse( false );
		}
		// get key and value block ids to clear out and set
		int prevProp = propRecord.getPrevProp();
		int nextProp = propRecord.getNextProp();
		if ( relRecord.getNextProp() == propertyId )
		{
			relRecord.setNextProp( nextProp );
			// re-adding not a problem
			addRelationshipRecord( relRecord );
		}
		if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
		{
			PropertyRecord prevPropRecord = getPropertyRecord( prevProp );
			if ( prevPropRecord == null )
			{
				prevPropRecord = getPropertyStore().getLightRecord( prevProp, 
					readFromBuffer );
				addPropertyRecord( prevPropRecord );
			}
			prevPropRecord.setNextProp( nextProp );
		}
		if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord nextPropRecord = getPropertyRecord( nextProp );
			if ( nextPropRecord == null )
			{
				nextPropRecord = getPropertyStore().getLightRecord( nextProp, 
					readFromBuffer );
				addPropertyRecord( nextPropRecord );
			}
			nextPropRecord.setPrevProp( prevProp );
		}
	}
	
	public PropertyData[] relGetProperties( int relId ) throws IOException
    {
		RelationshipRecord relRecord = getRelationshipRecord( relId );
		if ( relRecord == null )
		{
			relRecord = getRelationshipStore().getRecord( relId, 
				readFromBuffer );
		}
		int nextProp = relRecord.getNextProp();
		List<PropertyData> properties = new ArrayList<PropertyData>();
		while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord propRecord = getPropertyRecord( nextProp );
			if ( propRecord == null )
			{
				propRecord = getPropertyStore().getLightRecord( nextProp, 
					readFromBuffer );
			}
			properties.add( new PropertyData( propRecord.getId(), 
				propRecord.getKeyIndexId(), null ) );
			nextProp = propRecord.getNextProp();
		}
		return properties.toArray( new PropertyData[properties.size()] );
    }
	
	public PropertyData[] nodeGetProperties( int nodeId ) throws IOException
    {
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
		}
		int nextProp = nodeRecord.getNextProp();
		List<PropertyData> properties = new ArrayList<PropertyData>();
		while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord propRecord = getPropertyRecord( nextProp );
			if ( propRecord == null )
			{
				propRecord = getPropertyStore().getLightRecord( nextProp, 
					readFromBuffer );
			}
			properties.add( new PropertyData( propRecord.getId(), 
				propRecord.getKeyIndexId(), null ) );
			nextProp = propRecord.getNextProp();
		}
		return properties.toArray( new PropertyData[properties.size()] );
    }
	
	public Object propertyGetValue( int id ) throws IOException
    {
		PropertyRecord propertyRecord = getPropertyRecord( id );
		if ( propertyRecord == null )
		{
			propertyRecord = getPropertyStore().getRecord( id, readFromBuffer );
		}
		if ( propertyRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propertyRecord, readFromBuffer );
		}
		PropertyType type = propertyRecord.getType();
		if ( type == PropertyType.INT )
		{
			return (int) propertyRecord.getPropBlock();
		}
		if ( type == PropertyType.STRING )
		{
			return getPropertyStore().getStringFor( propertyRecord, 
				readFromBuffer );
		}
		if ( type == PropertyType.BOOL )
		{
			if ( propertyRecord.getPropBlock() == 1 )
			{
				return Boolean.valueOf( true );
			}
			return Boolean.valueOf( false );
		}
		if ( type == PropertyType.DOUBLE )
		{
			return new Double( Double.longBitsToDouble( 
				propertyRecord.getPropBlock() ) );
		}
		if ( type == PropertyType.FLOAT )
		{
			return new Float( Float.intBitsToFloat( 
				(int) propertyRecord.getPropBlock() ) );
		}
		if ( type == PropertyType.LONG )
		{
			return propertyRecord.getPropBlock();
		}
		if ( type == PropertyType.BYTE )
		{
			return (byte) propertyRecord.getPropBlock();
		}
		if ( type == PropertyType.CHAR )
		{
			return (char) propertyRecord.getPropBlock();
		}
		if ( type == PropertyType.ARRAY )
		{
			return getPropertyStore().getArrayFor( propertyRecord, 
				readFromBuffer );
		}
		throw new RuntimeException( "Unkown type: " + type );
    }
	
	void nodeRemoveProperty( int nodeId, int propertyId ) throws IOException
	{
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
		}
		PropertyRecord propRecord = getPropertyRecord( propertyId );
		if ( propRecord == null )
		{
			propRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propRecord );
		}
		if ( propRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propRecord, readFromBuffer );
		}
		
		propRecord.setInUse( false );
		// TODO: update count on property index record
//		for ( DynamicRecord keyRecord : propRecord.getKeyRecords() )
//		{
//			keyRecord.setInUse( false );
//		}
		for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
		{
			valueRecord.setInUse( false );
		}
		// get key and value block ids to clear out and set
		int prevProp = propRecord.getPrevProp();
		int nextProp = propRecord.getNextProp();
		if ( nodeRecord.getNextProp() == propertyId )
		{
			nodeRecord.setNextProp( nextProp );
			// re-adding not a problem
			addNodeRecord( nodeRecord );
		}
		if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
		{
			PropertyRecord prevPropRecord = getPropertyRecord( prevProp );
			if ( prevPropRecord == null )
			{
				prevPropRecord = getPropertyStore().getLightRecord( prevProp, 
					readFromBuffer );
			}
			prevPropRecord.setNextProp( nextProp );
			addPropertyRecord( prevPropRecord );
		}
		if ( nextProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
		{
			PropertyRecord nextPropRecord = getPropertyRecord( nextProp );
			if ( nextPropRecord == null )
			{
				nextPropRecord = getPropertyStore().getLightRecord( nextProp, 
					readFromBuffer );
			}
			nextPropRecord.setPrevProp( prevProp );
			addPropertyRecord( nextPropRecord );
		}
	}

	void relChangeProperty( int propertyId, Object value ) throws IOException
	{
		PropertyRecord propertyRecord = getPropertyRecord( propertyId );
		if ( propertyRecord == null )
		{
			propertyRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propertyRecord );
		}
		if ( propertyRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propertyRecord, readFromBuffer );
		}
		if ( propertyRecord.getType() == PropertyType.STRING )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				record.setInUse( false );
			}
		}
		getPropertyStore().encodeValue( propertyRecord, value );
		addPropertyRecord( propertyRecord );
	}
	
	void nodeChangeProperty( int propertyId, Object value )
		throws IOException
	{
		PropertyRecord propertyRecord = getPropertyRecord( propertyId );
		if ( propertyRecord == null )
		{
			propertyRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propertyRecord );
		}
		if ( propertyRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propertyRecord, readFromBuffer );
		}
		// TODO:
		if ( propertyRecord.getType() == PropertyType.STRING )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				record.setInUse( false );
			}
		}
		getPropertyStore().encodeValue( propertyRecord, value );
		addPropertyRecord( propertyRecord );
	}

	void relAddProperty( int relId, int propertyId, PropertyIndex index, 
		Object value ) throws IOException
	{
		RelationshipRecord relRecord = getRelationshipRecord( relId );
		if ( relRecord == null )
		{
			relRecord = getRelationshipStore().getRecord( relId, 
				readFromBuffer );
			addRelationshipRecord( relRecord );
		}
		
//		PropertyType type = getPropertyStore().getType( value );
//		PropertyRecord propertyRecord = new PropertyRecord( propertyId, 
//			type );
		PropertyRecord propertyRecord = new PropertyRecord( propertyId );
		propertyRecord.setInUse( true );
		propertyRecord.setCreated();
		if ( relRecord.getNextProp() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			PropertyRecord prevProp = getPropertyRecord( 
				relRecord.getNextProp() );
			if ( prevProp == null )
			{
				prevProp = getPropertyStore().getLightRecord( 
					relRecord.getNextProp(), readFromBuffer );
				addPropertyRecord( prevProp );
			}
			assert prevProp.getPrevProp() == 
				Record.NO_PREVIOUS_PROPERTY.intValue();
			prevProp.setPrevProp( propertyId );
			propertyRecord.setNextProp( prevProp.getId() );
		}
//		int keyBlockId = getPropertyStore().nextKeyBlockId();
//		propertyRecord.setKeyBlock( keyBlockId );
//		Collection<DynamicRecord> keyRecords = 
//			getPropertyStore().allocateKeyRecords( keyBlockId, 
//				index.getChars() );
//		for ( DynamicRecord keyRecord : keyRecords )
//		{
//			propertyRecord.addKeyRecord( keyRecord );
//		}
		int keyIndexId = index.getKeyId();
		propertyRecord.setKeyIndexId( keyIndexId );
		getPropertyStore().encodeValue( propertyRecord, value );
		relRecord.setNextProp( propertyId );
		addPropertyRecord( propertyRecord );
	}
	
	void nodeAddProperty( int nodeId, int propertyId, PropertyIndex index, 
		Object value ) throws IOException
	{
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
			addNodeRecord( nodeRecord );
		}
		
//		PropertyType type = getPropertyStore().getType( value );
//		PropertyRecord propertyRecord = new PropertyRecord( propertyId, 
//			type );
		PropertyRecord propertyRecord = new PropertyRecord( propertyId );
		propertyRecord.setInUse( true );
		propertyRecord.setCreated();
		if ( nodeRecord.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
		{
			PropertyRecord prevProp = getPropertyRecord( 
					nodeRecord.getNextProp() );
			if ( prevProp == null )
			{
				prevProp = getPropertyStore().getLightRecord( 
					nodeRecord.getNextProp(), readFromBuffer );
				addPropertyRecord( prevProp );
			}
			assert prevProp.getPrevProp() == 
				Record.NO_PREVIOUS_PROPERTY.intValue();
			prevProp.setPrevProp( propertyId );
			propertyRecord.setNextProp( prevProp.getId() );
		}
//		int keyBlockId = getPropertyStore().nextKeyBlockId();
//		propertyRecord.setKeyBlock( keyBlockId );
//		Collection<DynamicRecord> keyRecords = 
//			getPropertyStore().allocateKeyRecords( keyBlockId, 
//				index.getChars() );
//		for ( DynamicRecord keyRecord : keyRecords )
//		{
//			propertyRecord.addKeyRecord( keyRecord );
//		}
		int keyIndexId = index.getKeyId();
		propertyRecord.setKeyIndexId( keyIndexId );
		getPropertyStore().encodeValue( propertyRecord, value );
		nodeRecord.setNextProp( propertyId );
		addPropertyRecord( propertyRecord );
	}

	void relationshipCreate( int id, int firstNode, 
		int secondNode, int type ) throws IOException
	{
		RelationshipRecord record = new RelationshipRecord( id, 
			firstNode, secondNode, type );
		record.setInUse( true );
		record.setCreated();
		addRelationshipRecord( record );
		connectRelationship( record );
	}
	
	private void connectRelationship( RelationshipRecord rel ) 
		throws IOException 
	{
		NodeRecord firstNode = getNodeRecord( rel.getFirstNode() );
		if ( firstNode == null )
		{
			firstNode = getNodeStore().getRecord( rel.getFirstNode(), 
				readFromBuffer );
			addNodeRecord( firstNode );
		}
		NodeRecord secondNode = getNodeRecord( rel.getSecondNode() );
		if ( secondNode == null )
		{
			secondNode = getNodeStore().getRecord( rel.getSecondNode(), 
				readFromBuffer );
			addNodeRecord( secondNode );
		}
		assert firstNode.getNextRel() != rel.getId();
		assert secondNode.getNextRel() != rel.getId();
		rel.setFirstNextRel( firstNode.getNextRel() );
		rel.setSecondNextRel( secondNode.getNextRel() );
		if ( firstNode.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord nextRel = getRelationshipRecord( 
					firstNode.getNextRel() );
			if ( nextRel == null )
			{
				nextRel = getRelationshipStore().getRecord( 
					firstNode.getNextRel(), readFromBuffer );
				addRelationshipRecord( nextRel );
			}
			if ( nextRel.getFirstNode() == firstNode.getId() )
			{
				nextRel.setFirstPrevRel( rel.getId() );
			}
			else if ( nextRel.getSecondNode() == firstNode.getId() )
			{
				nextRel.setSecondPrevRel( rel.getId() );
			}
			else 
			{
				throw new RuntimeException( firstNode + " dont match " +
					nextRel );
			}
		}
		if ( secondNode.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipRecord nextRel = getRelationshipRecord( 
					secondNode.getNextRel() );
			if ( nextRel == null )
			{
				nextRel = getRelationshipStore().getRecord( 
					secondNode.getNextRel(), readFromBuffer );
				addRelationshipRecord( nextRel );
			}
			if ( nextRel.getFirstNode() == secondNode.getId() )
			{
				nextRel.setFirstPrevRel( rel.getId() );
			}
			else if ( nextRel.getSecondNode() == secondNode.getId() )
			{
				nextRel.setSecondPrevRel( rel.getId() );
			}
			else 
			{
				throw new RuntimeException( firstNode + " dont match " +
					nextRel );
			}
		}
		firstNode.setNextRel( rel.getId() );
		secondNode.setNextRel( rel.getId() );
	}
	
	void nodeCreate( int nodeId )
	{
		NodeRecord nodeRecord = new NodeRecord( nodeId );
		nodeRecord.setInUse( true );
		nodeRecord.setCreated();
		addNodeRecord( nodeRecord );
	}

	String getPropertyIndex( int id ) throws IOException
    {
		PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
		PropertyIndexRecord index = getPropertyIndexRecord( id );
		if ( index == null )
		{
			index = indexStore.getRecord( id, readFromBuffer ); 
		}
		if ( index.isLight() )
		{
			indexStore.makeHeavy( index, readFromBuffer );
		}
		return indexStore.getStringFor( index, readFromBuffer );
    }
	
	void createPropertyIndex( int id, String key ) throws IOException
	{
		PropertyIndexRecord record = new PropertyIndexRecord( id );
		record.setInUse( true );
		record.setCreated();
		PropertyIndexStore propIndexStore = getPropertyStore().getIndexStore();
		int keyBlockId = propIndexStore.nextKeyBlockId();
		record.setKeyBlockId( keyBlockId );
		int length = key.length();
		char[] chars = new char[length];
		key.getChars( 0, length, chars, 0 );
		Collection<DynamicRecord> keyRecords = 
			propIndexStore.allocateKeyRecords( keyBlockId, chars );
		for ( DynamicRecord keyRecord : keyRecords )
		{
			record.addKeyRecord( keyRecord );
		}
		addPropertyIndexRecord( record );
	}
	
	void relationshipTypeAdd( int id, String name ) throws IOException
	{
		RelationshipTypeRecord record = new RelationshipTypeRecord( id );
		record.setInUse( true );
		record.setCreated();
		int blockId = getRelationshipTypeStore().nextBlockId();
		record.setTypeBlock( blockId );
		Collection<DynamicRecord> typeNameRecords = 
			getRelationshipTypeStore().allocateTypeNameRecords( blockId, 
				name.getBytes() );
		for ( DynamicRecord typeRecord : typeNameRecords )
		{
			record.addTypeRecord( typeRecord );
		}
		addRelationshipTypeRecord( record );
	}

//	public boolean propertyDeleted( int propertyId )
//	{
//		if ( removedPropsMap.containsKey( propertyId ) || 
//			strayPropMap.containsKey( propertyId ) )
//		{
//			return true;
//		}
//		return false;
//	}
	
//	public boolean nodeCreated( int nodeId )
//	{
//		return createdNodesMap.containsKey( nodeId );
//	}

//	public boolean relationshipCreated( int relId )
//	{
//		return createdRelsMap.containsKey( relId );
//	}

//	public boolean relationshipTypeAdded( int id )
//	{
//		return createdRelTypesMap.containsKey( id );
//	}
	
//	public boolean nodeDeleted( int nodeId )
//	{
//		return deletedNodesMap.containsKey( nodeId );
//	}

//	public boolean relationshipDeleted( int relId )
//	{
//		return deletedRelsMap.containsKey( relId );
//	}
	
//	RelationshipData getCreatedRelationship( int id )
//	{
//		return ( ( MemCommand.RelationshipCreate ) 
//			createdRelsMap.get( id ) ).getRelationshipData();
//	}
	
	static class CommandSorter implements Comparator<Command>
	{
		public int compare( Command o1, Command o2 )
		{
			int id1 = o1.getKey().intValue();
			int id2 = o2.getKey().intValue();
			return id1 - id2;
		}
		
		public boolean equals( Object o )
		{
			if ( o instanceof CommandSorter )
			{
				return true;
			}
			return false;
		}
	
		public int hashCode()
		{
			return 3217;
		}
	}

	void addNodeRecord( NodeRecord record )
	{
		nodeRecords.put( record.getId(), record );
	}
	
	NodeRecord getNodeRecord( int nodeId )
	{
		return nodeRecords.get( nodeId );
	}

	void addRelationshipRecord( RelationshipRecord record )
	{
		relRecords.put( record.getId(), record );
	}
	
	RelationshipRecord getRelationshipRecord( int relId )
	{
		return relRecords.get( relId );
	}
	
	void addPropertyRecord( PropertyRecord record )
	{
		propertyRecords.put( record.getId(), record );
	}

	PropertyRecord getPropertyRecord( int propertyId )
	{
		return propertyRecords.get( propertyId );
	}
	
	void addRelationshipTypeRecord( RelationshipTypeRecord record )
	{
		relTypeRecords.put( record.getId(), record );
	}

	void addPropertyIndexRecord( PropertyIndexRecord record )
	{
		propIndexRecords.put( record.getId(), record );
	}

	PropertyIndexRecord getPropertyIndexRecord( int id )
	{
		return propIndexRecords.get( id );
	}
}
