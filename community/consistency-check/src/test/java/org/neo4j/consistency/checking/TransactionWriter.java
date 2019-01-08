/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.kernel.impl.store.NeoStores;
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
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.schema.SchemaRule;

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
    private final NeoStores neoStores;

    private final List<Command.NodeCommand> nodeCommands = new ArrayList<>();
    private final List<Command.RelationshipCommand> relationshipCommands = new ArrayList<>();
    private final List<Command.RelationshipGroupCommand> relationshipGroupCommands = new ArrayList<>();
    private final List<Command> otherCommands = new ArrayList<>();

    public TransactionWriter( NeoStores neoStores )
    {
        this.neoStores = neoStores;
    }

    public TransactionRepresentation representation( byte[] additionalHeader, int masterId, int authorId,
            long startTime, long lastCommittedTx, long committedTime )
    {
        prepareForCommit();
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( allCommands() );
        representation.setHeader( additionalHeader, masterId, authorId, startTime, lastCommittedTx, committedTime, -1 );
        return representation;
    }

    public void propertyKey( int id, String key, int... dynamicIds )
    {
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( id );
        PropertyKeyTokenRecord after = withName( new PropertyKeyTokenRecord( id ), dynamicIds, key );
        otherCommands.add( new Command.PropertyKeyTokenCommand( before, after ) );
    }

    public void label( int id, String name, int... dynamicIds )
    {
        LabelTokenRecord before = new LabelTokenRecord( id );
        LabelTokenRecord after = withName( new LabelTokenRecord( id ), dynamicIds, name );
        otherCommands.add( new Command.LabelTokenCommand( before, after ) );
    }

    public void relationshipType( int id, String label, int... dynamicIds )
    {
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( id );
        RelationshipTypeTokenRecord after = withName( new RelationshipTypeTokenRecord( id ), dynamicIds, label );
        otherCommands.add( new Command.RelationshipTypeTokenCommand( before, after ) );
    }

    public void update( NeoStoreRecord before, NeoStoreRecord after )
    {
        otherCommands.add( new Command.NeoStoreCommand( before, after ) );
    }

    public void update( LabelTokenRecord before, LabelTokenRecord after )
    {
        otherCommands.add( new Command.LabelTokenCommand( before, after ) );
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
        update( new LabelTokenRecord( labelToken.getIntId() ), labelToken );
    }

    public void create( PropertyKeyTokenRecord token )
    {
        token.setCreated();
        update( new PropertyKeyTokenRecord( token.getIntId() ), token );
    }

    public void create( RelationshipGroupRecord group )
    {
        group.setCreated();
        update( new RelationshipGroupRecord( group.getId(), group.getType() ), group );
    }

    public void update( NodeRecord before, NodeRecord node )
    {
        node.setInUse( true );
        add( before, node );
    }

    public void update( PropertyKeyTokenRecord before, PropertyKeyTokenRecord after )
    {
        after.setInUse( true );
        add( before, after );
    }

    public void delete( NodeRecord node )
    {
        node.setInUse( false );
        add( node, new NodeRecord( node.getId(), false, NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() ) );
    }

    public void create(  RelationshipRecord record  )
    {
        record.setCreated();
        update( new RelationshipRecord( record.getId() ), record );
    }

    public void delete( RelationshipGroupRecord group )
    {
        group.setInUse( false );
        add( group, new RelationshipGroupRecord( group.getId(), group.getType() ) );
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

    public void update( RelationshipRecord before, RelationshipRecord after )
    {
        after.setInUse( true );
        add( before, after );
    }

    public void update( RelationshipGroupRecord before, RelationshipGroupRecord after )
    {
        after.setInUse( true );
        add( before, after );
    }

    public void delete(  RelationshipRecord record  )
    {
        record.setInUse( false );
        add( record, new RelationshipRecord( record.getId() ) );
    }

    public void create( PropertyRecord property )
    {
        property.setCreated();
        PropertyRecord before = new PropertyRecord( property.getId() );
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
        otherCommands.add( new Command.SchemaRuleCommand( beforeRecords, afterRecords, rule ) );
    }

    public void add( NodeRecord before, NodeRecord after )
    {
        nodeCommands.add( new Command.NodeCommand( before, after ) );
    }

    public void add( RelationshipRecord before, RelationshipRecord after )
    {
        relationshipCommands.add( new Command.RelationshipCommand( before, after ) );
    }

    public void add( RelationshipGroupRecord before, RelationshipGroupRecord after )
    {
        relationshipGroupCommands.add( new Command.RelationshipGroupCommand( before, after ) );
    }

    public void add( PropertyRecord before, PropertyRecord property )
    {
        otherCommands.add( new Command.PropertyCommand( before, property ) );
    }

    public void add( RelationshipTypeTokenRecord before, RelationshipTypeTokenRecord after )
    {
        otherCommands.add( new Command.RelationshipTypeTokenCommand( before, after ) );
    }

    public void add( PropertyKeyTokenRecord before, PropertyKeyTokenRecord after )
    {
        otherCommands.add( new Command.PropertyKeyTokenCommand( before, after ) );
    }

    public void add( NeoStoreRecord before, NeoStoreRecord after )
    {
        otherCommands.add( new Command.NeoStoreCommand( before, after ) );
    }

    public void incrementNodeCount( int labelId, long delta )
    {
        otherCommands.add( new Command.NodeCountsCommand( labelId, delta ) );
    }

    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        otherCommands.add( new Command.RelationshipCountsCommand( startLabelId, typeId, endLabelId, delta ) );
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

    private void prepareForCommit()
    {
        for ( Command.NodeCommand command : nodeCommands )
        {
            neoStores.getNodeStore().prepareForCommit( command.getAfter() );
        }
        for ( Command.RelationshipCommand command : relationshipCommands )
        {
            neoStores.getRelationshipStore().prepareForCommit( command.getAfter() );
        }
        for ( Command.RelationshipGroupCommand command : relationshipGroupCommands )
        {
            neoStores.getRelationshipGroupStore().prepareForCommit( command.getAfter() );
        }
    }

    private List<StorageCommand> allCommands()
    {
        List<StorageCommand> allCommands = new ArrayList<>();
        allCommands.addAll( nodeCommands );
        allCommands.addAll( relationshipCommands );
        allCommands.addAll( relationshipGroupCommands );
        allCommands.addAll( otherCommands );
        return allCommands;
    }
}
