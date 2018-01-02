/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.neo4j.kernel.impl.store.TokenStore.NAME_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_PREV_RELATIONSHIP;

/**
 * This class lives here instead of somewhere else in order to be able to access the {@link Command} implementations.
 *
 * @author Tobias Lindaaker
 */
public class TransactionWriter
{
    private final List<Command> commands = new ArrayList<>();

    public TransactionRepresentation representation( byte[] additionalHeader, int masterId, int authorId,
            long startTime, long lastCommittedTx, long committedTime )
    {
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( additionalHeader, masterId, authorId, startTime, lastCommittedTx, committedTime, -1 );
        return representation;
    }

    public void propertyKey( int id, String key, int... dynamicIds )
    {
        Command.PropertyKeyTokenCommand command = new Command.PropertyKeyTokenCommand();
        command.init( withName( new PropertyKeyTokenRecord( id ), dynamicIds, key ) );
        addCommand( command );
    }

    public void label( int id, String name, int... dynamicIds )
    {
        Command.LabelTokenCommand command = new Command.LabelTokenCommand();
        command.init( withName( new LabelTokenRecord( id ), dynamicIds, name ) );
        addCommand( command );
    }

    public void relationshipType( int id, String label, int... dynamicIds )
    {
        Command.RelationshipTypeTokenCommand command = new Command.RelationshipTypeTokenCommand();
        command.init( withName( new RelationshipTypeTokenRecord( id ), dynamicIds, label ) );
        addCommand( command );
    }

    public void update( NeoStoreRecord record )
    {
        Command.NeoStoreCommand command = new Command.NeoStoreCommand();
        command.init( record );
        addCommand( command );
    }

    public void update( LabelTokenRecord labelToken )
    {
        Command.LabelTokenCommand command = new Command.LabelTokenCommand();
        command.init( labelToken );
        addCommand( command );
    }

    private void addCommand( Command command )
    {
        this.commands.add( command );
    }

    public void create( NodeRecord node )
    {
        node.setCreated();
        update( new NodeRecord( node.getId(), false, NO_PREV_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue() ), node );
    }

    public void create( LabelTokenRecord labelToken )
    {
        labelToken.setCreated();
        update( labelToken );
    }

    public void create( PropertyKeyTokenRecord token )
    {
        token.setCreated();
        update( token );
    }

    public void create( RelationshipGroupRecord group )
    {
        group.setCreated();
        update( group );
    }

    public void update( NodeRecord before, NodeRecord node )
    {
        node.setInUse( true );
        add( before, node );
    }

    public void update( PropertyKeyTokenRecord token )
    {
        token.setInUse( true );
        add( token );
    }

    public void delete( NodeRecord node )
    {
        node.setInUse( false );
        add( node, new NodeRecord( node.getId(), false, NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() ) );
    }

    public void create( RelationshipRecord relationship )
    {
        relationship.setCreated();
        update( relationship );
    }

    public void delete( RelationshipGroupRecord group )
    {
        group.setInUse( false );
        add( group );
    }

    public void createSchema( Collection<DynamicRecord> beforeRecord, Collection<DynamicRecord> afterRecord,
            SchemaRule rule )
    {
        for ( DynamicRecord record : afterRecord )
        {
            record.setCreated();
        }
        updateSchema( beforeRecord, afterRecord, rule );
    }

    public void updateSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords,
            SchemaRule rule )
    {
        for ( DynamicRecord record : afterRecords )
        {
            record.setInUse( true );
        }
        addSchema( beforeRecords, afterRecords, rule );
    }

    public void update( RelationshipRecord relationship )
    {
        relationship.setInUse( true );
        add( relationship );
    }

    public void update( RelationshipGroupRecord group )
    {
        group.setInUse( true );
        add( group );
    }

    public void delete( RelationshipRecord relationship )
    {
        relationship.setInUse( false );
        add( relationship );
    }

    public void create( PropertyRecord property )
    {
        property.setCreated();
        PropertyRecord before = new PropertyRecord( property.getLongId() );
        if ( property.isNodeSet() )
        {
            before.setNodeId( property.getNodeId() );
        }
        if ( property.isRelSet() )
        {
            before.setRelId( property.getRelId() );
        }
        update( before, property );
    }

    public void update( PropertyRecord before, PropertyRecord after )
    {
        after.setInUse(true);
        add( before, after );
    }

    public void delete( PropertyRecord before, PropertyRecord after )
    {
        after.setInUse(false);
        add( before, after );
    }

    // Internals

    private void addSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords,
            SchemaRule rule )
    {
        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, rule );
        addCommand( command );
    }

    public void add( NodeRecord before, NodeRecord after )
    {
        Command.NodeCommand command = new Command.NodeCommand();
        command.init(  before, after );
        addCommand( command );
    }

    public void add( RelationshipRecord relationship )
    {
        Command.RelationshipCommand command = new Command.RelationshipCommand();
        command.init( relationship );
        addCommand( command );
    }

    public void add( RelationshipGroupRecord group )
    {
        Command.RelationshipGroupCommand command = new Command.RelationshipGroupCommand();
        command.init( group );
        addCommand( command );
    }

    public void add( PropertyRecord before, PropertyRecord property )
    {
        Command.PropertyCommand command = new Command.PropertyCommand();
        command.init( before, property );
        addCommand( command );
    }

    public void add( RelationshipTypeTokenRecord record )
    {
        Command.RelationshipTypeTokenCommand command = new Command.RelationshipTypeTokenCommand();
        command.init( record );
        addCommand( command );
    }

    public void add( PropertyKeyTokenRecord record )
    {
        Command.PropertyKeyTokenCommand command = new Command.PropertyKeyTokenCommand();
        command.init( record );
        addCommand( command );
    }

    public void add( NeoStoreRecord record )
    {
        Command.NeoStoreCommand command = new Command.NeoStoreCommand();
        command.init( record );
        addCommand( command );
    }

    public void incrementNodeCount( int labelId, long delta )
    {
        addCommand( new Command.NodeCountsCommand().init( labelId, delta ) );
    }

    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        addCommand( new Command.RelationshipCountsCommand().init( startLabelId, typeId, endLabelId, delta ) );
    }

    private static <T extends TokenRecord> T withName( T record, int[] dynamicIds, String name )
    {
        if ( dynamicIds == null || dynamicIds.length == 0 )
        {
            throw new IllegalArgumentException( "No dynamic records for storing the name." );
        }
        record.setInUse( true );
        byte[] data = PropertyStore.encodeString( name );
        if ( data.length > dynamicIds.length * NAME_STORE_BLOCK_SIZE )
        {
            throw new IllegalArgumentException(
                    String.format( "[%s] is too long to fit in %d blocks", name, dynamicIds.length ) );
        }
        else if ( data.length <= (dynamicIds.length - 1) * NAME_STORE_BLOCK_SIZE )
        {
            throw new IllegalArgumentException(
                    String.format( "[%s] is to short to fill %d blocks", name, dynamicIds.length ) );
        }

        for ( int i = 0; i < dynamicIds.length; i++ )
        {
            byte[] part = new byte[Math.min( NAME_STORE_BLOCK_SIZE, data.length - i * NAME_STORE_BLOCK_SIZE )];
            System.arraycopy( data, i * NAME_STORE_BLOCK_SIZE, part, 0, part.length );

            DynamicRecord dynamicRecord = new DynamicRecord( dynamicIds[i] );
            dynamicRecord.setInUse( true );
            dynamicRecord.setData( part );
            dynamicRecord.setCreated();
            record.addNameRecord( dynamicRecord );
        }
        record.setNameId( dynamicIds[0] );
        return record;
    }
}
