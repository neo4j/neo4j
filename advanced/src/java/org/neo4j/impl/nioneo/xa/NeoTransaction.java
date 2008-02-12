/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.XAException;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.core.RawPropertyIndex;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
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
import org.neo4j.impl.nioneo.store.StoreFailureException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.impl.transaction.xaframework.XaTransaction;

/**
 * Transaction containing {@link Command commands} reflecting the operations
 * performed in the transaction.
 */
class NeoTransaction extends XaTransaction
{
	private final Map<Integer,NodeRecord> nodeRecords = 
		new HashMap<Integer,NodeRecord>();
	private final Map<Integer,PropertyRecord> propertyRecords = 
		new HashMap<Integer,PropertyRecord>();
	private final Map<Integer,RelationshipRecord> relRecords = 
		new HashMap<Integer,RelationshipRecord>();
	private final Map<Integer,RelationshipTypeRecord> relTypeRecords =
		new HashMap<Integer,RelationshipTypeRecord>();
	private final Map<Integer,PropertyIndexRecord> propIndexRecords =
		new HashMap<Integer,PropertyIndexRecord>();
	
	private final ArrayList<Command.NodeCommand> nodeCommands = 
		new ArrayList<Command.NodeCommand>();
	private final ArrayList<Command.PropertyCommand> propCommands = 
		new ArrayList<Command.PropertyCommand>();
	private final ArrayList<Command.PropertyIndexCommand> propIndexCommands = 
		new ArrayList<Command.PropertyIndexCommand>();
	private final ArrayList<Command.RelationshipCommand> relCommands = 
		new ArrayList<Command.RelationshipCommand>();
	private final ArrayList<Command.RelationshipTypeCommand> relTypeCommands = 
		new ArrayList<Command.RelationshipTypeCommand>();
	
	private final NeoStore neoStore;
	private final ReadFromBuffer readFromBuffer;
	private boolean committed = false;
	private boolean prepared = false;
	
	private final LockReleaser lockReleaser;
	private final LockManager lockManager;
	private final EventManager eventManager;
	
	NeoTransaction( int identifier, XaLogicalLog log, NeoStore neoStore, 
		LockReleaser lockReleaser, LockManager lockManager, 
		EventManager eventManager )
	{
		super( identifier, log );
		this.neoStore = neoStore;
		this.readFromBuffer = neoStore.getNewReadFromBuffer();
		this.lockReleaser = lockReleaser;
		this.lockManager = lockManager;
		this.eventManager = eventManager;
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
		if ( nodeRecords.size() == 0 && relRecords.size() == 0 && 
			relTypeRecords.size() == 0 && propertyRecords.size() == 0 && 
			propIndexRecords.size() == 0 )
		{
			return true;
		}
		return false;
	}
	
	public void doAddCommand( XaCommand command )
	{
		// do nothing, commands are created and added in prepare
	}
	
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
			if ( !record.inUse() && record.getNextRel() != 
				Record.NO_NEXT_RELATIONSHIP.intValue() )
			{
				throw new StoreFailureException( "Node record " + record + 
					" still has relationships" );
//				assert record.getNextRel() == 
//					Record.NO_NEXT_RELATIONSHIP.intValue();
			}
			Command.NodeCommand command = new Command.NodeCommand( 
				neoStore.getNodeStore(), record );
			nodeCommands.add( command );
			addCommand( command );
            if ( !record.inUse() )
            {
                removeNodeFromCache( record.getId() );
            }
		}
		for ( RelationshipRecord record : relRecords.values() )
		{
			Command.RelationshipCommand command = 
				new Command.RelationshipCommand( 
					neoStore.getRelationshipStore(), record );
			relCommands.add( command );
			addCommand( command );
            if ( !record.inUse() )
            {
                removeRelationshipFromCache( record.getId() );
            }
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
				removeRelationshipTypeFromCache( record.getId() );
			}
			for ( NodeRecord record : nodeRecords.values() )
			{
				if ( record.isCreated() )
				{
					getNodeStore().freeId( record.getId() );
				}
				removeNodeFromCache( record.getId() );
			}
			for ( RelationshipRecord record : relRecords.values() )
			{
				if ( record.isCreated() )
				{
					getRelationshipStore().freeId( record.getId() );
				}
				removeRelationshipFromCache( record.getId() );
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
				if ( record.getNodeId() != -1 )
				{
					removeNodeFromCache( record.getNodeId() );
				}
				else if ( record.getRelId() != -1 )
				{
					removeRelationshipFromCache( record.getRelId() );
				}
				if ( record.isCreated() )
				{
					getPropertyStore().freeId( record.getId() );
					for ( DynamicRecord dynamicRecord: 
						record.getValueRecords() )
					{
						if ( dynamicRecord.isCreated() )
						{
							if ( dynamicRecord.getType() == 
								PropertyType.STRING.intValue() )
							{
								getPropertyStore().freeStringBlockId(  
									dynamicRecord.getId() );
							}
							else if ( dynamicRecord.getType() == 
								PropertyType.ARRAY.intValue() )
							{
								getPropertyStore().freeArrayBlockId( 
									dynamicRecord.getId() );
							}
							else
							{
								throw new RuntimeException( "Unkown type" );
							}
						}
					}
				}
			}
		}
        finally
        {
            nodeRecords.clear();
            propertyRecords.clear();
            relRecords.clear();
            relTypeRecords.clear();
            propIndexRecords.clear();
            
            nodeCommands.clear();
            propCommands.clear();
            propIndexCommands.clear();
            relCommands.clear();
            relTypeCommands.clear();
        }
	}
	
	private void removeRelationshipTypeFromCache( int id )
    {
		eventManager.generateProActiveEvent( Event.PURGE_REL_TYPE, 
			new EventData( id ) );
    }

	private void removeRelationshipFromCache( int id )
    {
		eventManager.generateProActiveEvent( Event.PURGE_REL, 
			new EventData( id ) );
    }
	
	private void removeNodeFromCache( int id )
    {
		eventManager.generateProActiveEvent( Event.PURGE_NODE, 
			new EventData( id ) );
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
		finally
		{
			TxInfoManager.getManager().unregisterMode();
            nodeRecords.clear();
            propertyRecords.clear();
            relRecords.clear();
            relTypeRecords.clear();
            propIndexRecords.clear();
            
            nodeCommands.clear();
            propCommands.clear();
            propIndexCommands.clear();
            relCommands.clear();
            relTypeCommands.clear();
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
	
	public boolean nodeLoadLight( int nodeId )
    {
	    NodeRecord nodeRecord = getNodeRecord( nodeId );
	    if ( nodeRecord != null )
	    {
	    	return nodeRecord.inUse();
	    }
		return getNodeStore().loadLightNode( nodeId, readFromBuffer );
    }

	public RelationshipData relationshipLoad( int id )
    {
	    RelationshipRecord relRecord = getRelationshipRecord( id );
	    if ( relRecord != null )
	    {
	    	if ( !relRecord.inUse() )
	    	{
	    		throw new StoreFailureException( "Relationship[" + id + 
                    "] not in use" );
	    	}
	    	return new RelationshipData( id, relRecord.getFirstNode(), 
	    		relRecord.getSecondNode(), relRecord.getType() );
	    }
	    relRecord = getRelationshipStore().getRecord( id, readFromBuffer );
    	return new RelationshipData( id, relRecord.getFirstNode(), 
    		relRecord.getSecondNode(), relRecord.getType() );
    }
	
	void nodeDelete( int nodeId )
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
			for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
			{
				valueRecord.setInUse( false );
			}
		}
	}

	void relDelete( int id )
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
	{
		// update first node prev
		if ( rel.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			Relationship lockableRel = new LockableRelationship( 
				rel.getFirstPrevRel() );
			getWriteLock( lockableRel );
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
			Relationship lockableRel = new LockableRelationship( 
				rel.getFirstNextRel() );
			getWriteLock( lockableRel );
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
			Relationship lockableRel = new LockableRelationship( 
				rel.getSecondPrevRel() );
			getWriteLock( lockableRel );
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
			Relationship lockableRel = new LockableRelationship( 
				rel.getSecondNextRel() );
			getWriteLock( lockableRel );
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
	
	private void getWriteLock( Relationship lockableRel )
	{
		lockManager.getWriteLock( lockableRel );
		lockReleaser.addLockToTransaction( lockableRel, LockType.WRITE );
	}
	
	public RelationshipData[] nodeGetRelationships( int nodeId )
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
	
	private void updateNodes( RelationshipRecord rel )
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
	
	void relRemoveProperty( int relId, int propertyId )
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
		propRecord.setRelId( relId );
		if ( propRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propRecord, readFromBuffer );
		}
		
		propRecord.setInUse( false );
		// TODO: update count on property index record
		for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
		{
			if ( valueRecord.inUse() )
			{
				valueRecord.setInUse( false, propRecord.getType().intValue() );
			}
		}
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
	
	public PropertyData[] relGetProperties( int relId )
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
	
	public PropertyData[] nodeGetProperties( int nodeId )
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
	
	public Object propertyGetValue( int id )
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
		if ( type == PropertyType.SHORT )
		{
			return (short) propertyRecord.getPropBlock();
		}
		throw new RuntimeException( "Unkown type: " + type );
    }
	
	void nodeRemoveProperty( int nodeId, int propertyId )
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
		propRecord.setNodeId( nodeId );
		if ( propRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propRecord, readFromBuffer );
		}
		
		propRecord.setInUse( false );
		// TODO: update count on property index record
		for ( DynamicRecord valueRecord : propRecord.getValueRecords() )
		{
			if ( valueRecord.inUse() )
			{
				valueRecord.setInUse( false, propRecord.getType().intValue() );
			}
		}
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
		if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
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

	void relChangeProperty( int relId, int propertyId, Object value ) 
	{
		PropertyRecord propertyRecord = getPropertyRecord( propertyId );
		if ( propertyRecord == null )
		{
			propertyRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propertyRecord );
		}
		propertyRecord.setRelId( relId );
		if ( propertyRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propertyRecord, readFromBuffer );
		}
		if ( propertyRecord.getType() == PropertyType.STRING )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				if ( record.inUse() )
				{
					record.setInUse( false, PropertyType.STRING.intValue() );
				}
			}
		}
		else if ( propertyRecord.getType() == PropertyType.ARRAY )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				if ( record.inUse() )
				{
					record.setInUse( false, PropertyType.ARRAY.intValue() );
				}
			}
		}
		getPropertyStore().encodeValue( propertyRecord, value );
		addPropertyRecord( propertyRecord );
	}
	
	void nodeChangeProperty( int nodeId, int propertyId, Object value )
	{
		PropertyRecord propertyRecord = getPropertyRecord( propertyId );
		if ( propertyRecord == null )
		{
			propertyRecord = getPropertyStore().getRecord( propertyId, 
				readFromBuffer );
			addPropertyRecord( propertyRecord );
		}
		propertyRecord.setNodeId( nodeId );
		if ( propertyRecord.isLight() )
		{
			getPropertyStore().makeHeavy( propertyRecord, readFromBuffer );
		}
		if ( propertyRecord.getType() == PropertyType.STRING )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				if ( record.inUse() )
				{
					record.setInUse( false, PropertyType.STRING.intValue() );
				}
			}
		}
		else if ( propertyRecord.getType() == PropertyType.ARRAY )
		{
			for ( DynamicRecord record : propertyRecord.getValueRecords() )
			{
				if ( record.inUse() )
				{
					record.setInUse( false, PropertyType.ARRAY.intValue() );
				}
			}
		}
		getPropertyStore().encodeValue( propertyRecord, value );
		addPropertyRecord( propertyRecord );
	}

	void relAddProperty( int relId, int propertyId, PropertyIndex index, 
		Object value )
	{
		RelationshipRecord relRecord = getRelationshipRecord( relId );
		if ( relRecord == null )
		{
			relRecord = getRelationshipStore().getRecord( relId, 
				readFromBuffer );
			addRelationshipRecord( relRecord );
		}
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
		int keyIndexId = index.getKeyId();
		propertyRecord.setKeyIndexId( keyIndexId );
		getPropertyStore().encodeValue( propertyRecord, value );
		relRecord.setNextProp( propertyId );
		addPropertyRecord( propertyRecord );
	}
	
	void nodeAddProperty( int nodeId, int propertyId, PropertyIndex index, 
		Object value )
	{
		NodeRecord nodeRecord = getNodeRecord( nodeId );
		if ( nodeRecord == null )
		{
			nodeRecord = getNodeStore().getRecord( nodeId, readFromBuffer );
			addNodeRecord( nodeRecord );
		}
		
		PropertyRecord propertyRecord = new PropertyRecord( propertyId );
		propertyRecord.setInUse( true );
		propertyRecord.setCreated();
        // encoding has to be set here before anything is change
        // (exception is thrown in encodeValue now and tx not marked
        // rollback only
        getPropertyStore().encodeValue( propertyRecord, value );
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
		int keyIndexId = index.getKeyId();
		propertyRecord.setKeyIndexId( keyIndexId );
		nodeRecord.setNextProp( propertyId );
		addPropertyRecord( propertyRecord );
	}

	void relationshipCreate( int id, int firstNode, 
		int secondNode, int type )
	{
		RelationshipRecord record = new RelationshipRecord( id, 
			firstNode, secondNode, type );
		record.setInUse( true );
		record.setCreated();
		addRelationshipRecord( record );
		connectRelationship( record );
	}
	
	private void connectRelationship( RelationshipRecord rel ) 
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
			Relationship lockableRel = new LockableRelationship( 
				firstNode.getNextRel() );
			getWriteLock( lockableRel );
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
			Relationship lockableRel = new LockableRelationship( 
				secondNode.getNextRel() );
			getWriteLock( lockableRel );
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

	String getPropertyIndex( int id )
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
	
	RawPropertyIndex[] getPropertyIndexes( int count )
    {
		PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
		return indexStore.getPropertyIndexes( count );
    }
	
	void createPropertyIndex( int id, String key )
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
	
	void relationshipTypeAdd( int id, String name )
	{
		RelationshipTypeRecord record = new RelationshipTypeRecord( id );
		record.setInUse( true );
		record.setCreated();
		int blockId = getRelationshipTypeStore().nextBlockId();
		record.setTypeBlock( blockId );
		int length = name.length();
		char[] chars = new char[length];
		name.getChars( 0, length, chars, 0 );
		Collection<DynamicRecord> typeNameRecords = 
			getRelationshipTypeStore().allocateTypeNameRecords( blockId, 
				chars );
		for ( DynamicRecord typeRecord : typeNameRecords )
		{
			record.addTypeRecord( typeRecord );
		}
		addRelationshipTypeRecord( record );
	}

	static class CommandSorter implements Comparator<Command>
	{
		public int compare( Command o1, Command o2 )
		{
			int id1 = o1.getKey();
			int id2 = o2.getKey();
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
	
	private static class LockableRelationship implements Relationship
	{
		private final int id;
		
		LockableRelationship( int id )
		{
			this.id = id;
		}
		
		public void delete()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Node getEndNode()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public long getId()
        {
			return this.id;
        }

		public Node[] getNodes()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Node getOtherNode( Node node )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Object getProperty( String key )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Object getProperty( String key, Object defaultValue )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Iterable<String> getPropertyKeys()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Iterable<Object> getPropertyValues()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Node getStartNode()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public RelationshipType getType()
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }
		
		public boolean isType( RelationshipType type )
		{
			throw new UnsupportedOperationException( "Lockable rel" );			
		}

		public boolean hasProperty( String key )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public Object removeProperty( String key )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public void setProperty( String key, Object value )
        {
			throw new UnsupportedOperationException( "Lockable rel" );
        }

		public boolean equals( Object o )
		{
			if ( !(o instanceof Relationship) )
			{
				return false;
			}
			return this.getId() == ((Relationship) o).getId();
		}
		
		public int hashCode()
		{
			return id;
		}

		public String toString()
		{
			return	"Lockable relationship #" + this.getId();
		}
	}
}