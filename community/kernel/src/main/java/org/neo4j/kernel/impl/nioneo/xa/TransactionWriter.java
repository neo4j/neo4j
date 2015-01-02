/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.util.Collection;
import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_PREV_RELATIONSHIP;
import static org.neo4j.kernel.impl.nioneo.store.TokenStore.NAME_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;

/**
 * This class lives here instead of somewhere else in order to be able to access the {@link Command} implementations.
 *
 * @author Tobias Lindaaker
 */
public class TransactionWriter
{
    private final LogBuffer buffer;
    private final int identifier;
    private final int localId;

    public TransactionWriter( LogBuffer buffer, int identifier, int localId )
    {
        this.buffer = buffer;
        this.identifier = identifier;
        this.localId = localId;
    }

    // Transaction coordination

    public void start( int masterId, int myId, long latestCommittedTxWhenTxStarted ) throws IOException
    {
        start( getNewGlobalId( DEFAULT_SEED, localId ), masterId, myId, currentTimeMillis(), latestCommittedTxWhenTxStarted );
    }

    public void start( byte[] globalId, int masterId, int myId, long startTimestamp,
                       long latestCommittedTxWhenTxStarted ) throws IOException
    {
        Xid xid = new XidImpl( globalId, NeoStoreXaDataSource.BRANCH_ID );
        LogIoUtils.writeStart( buffer, this.identifier, xid, masterId, myId, startTimestamp, latestCommittedTxWhenTxStarted );
    }

    public void prepare() throws IOException
    {
        prepare( System.currentTimeMillis() );
    }

    public void prepare( long prepareTimestamp ) throws IOException
    {
        LogIoUtils.writePrepare( buffer, identifier, prepareTimestamp );
    }

    public void commit( boolean twoPhase, long txId ) throws IOException
    {
        commit( twoPhase, txId, System.currentTimeMillis() );
    }

    public void commit( boolean twoPhase, long txId, long commitTimestamp ) throws IOException
    {
        LogIoUtils.writeCommit( twoPhase, buffer, identifier, txId, commitTimestamp );
    }

    public void done() throws IOException
    {
        LogIoUtils.writeDone( buffer, identifier );
    }

    // Transaction data

    public void propertyKey( int id, String key, int... dynamicIds ) throws IOException
    {
        write( new Command.PropertyKeyTokenCommand( null, withName( new PropertyKeyTokenRecord( id ), dynamicIds, key ) ) );
    }

    public void label( int id, String name, int... dynamicIds ) throws IOException
    {
        write( new Command.LabelTokenCommand( null, withName( new LabelTokenRecord( id ), dynamicIds, name ) ) );
    }

    public void relationshipType( int id, String label, int... dynamicIds ) throws IOException
    {
        write( new Command.RelationshipTypeTokenCommand( null,
                withName( new RelationshipTypeTokenRecord( id ), dynamicIds, label ) ) );
    }

    public void update( NeoStoreRecord record ) throws IOException
    {
        write( new Command.NeoStoreCommand( null, record ) );
    }

    public void create( NodeRecord node ) throws IOException
    {
        node.setCreated();
        update( new NodeRecord( node.getId(), NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() ), node );
    }

    public void update( NodeRecord before, NodeRecord node ) throws IOException
    {
        node.setInUse( true );
        add( before, node );
    }

    public void delete( NodeRecord node ) throws IOException
    {
        node.setInUse( false );
        add( node, new NodeRecord( node.getId(), NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() ) );
    }

    public void create( RelationshipRecord relationship ) throws IOException
    {
        relationship.setCreated();
        update( relationship );
    }

    public void createSchema( Collection<DynamicRecord> beforeRecord, Collection<DynamicRecord> afterRecord ) throws IOException
    {
        for ( DynamicRecord record : afterRecord )
        {
            record.setCreated();
        }
        updateSchema( beforeRecord, afterRecord );
    }

    public void updateSchema(Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords) throws IOException
    {
        for ( DynamicRecord record : afterRecords )
        {
            record.setInUse( true );
        }
        addSchema( beforeRecords, afterRecords );
    }

    public void update( RelationshipRecord relationship ) throws IOException
    {
        relationship.setInUse( true );
        add( relationship );
    }

    public void delete( RelationshipRecord relationship ) throws IOException
    {
        relationship.setInUse( false );
        add( relationship );
    }

    public void create( PropertyRecord property ) throws IOException
    {
        property.setCreated();
        PropertyRecord before = new PropertyRecord( property.getLongId() );
        if ( property.isNodeSet() )
            before.setNodeId( property.getNodeId() );
        if ( property.isRelSet() )
            before.setRelId( property.getRelId() );
        update( before, property );
    }

    public void update( PropertyRecord before, PropertyRecord after ) throws IOException
    {
        after.setInUse(true);
        add( before, after );
    }

    public void delete( PropertyRecord before, PropertyRecord after ) throws IOException
    {
        after.setInUse(false);
        add( before, after );
    }

    // Internals

    private void addSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords ) throws IOException
    {
        write( new Command.SchemaRuleCommand( null, null, null, beforeRecords, afterRecords, null, Long.MAX_VALUE ) );
    }

    public void add( NodeRecord before, NodeRecord after ) throws IOException
    {
        write( new Command.NodeCommand( null, before, after ) );
    }

    public void add( RelationshipRecord relationship ) throws IOException
    {
        write( new Command.RelationshipCommand( null, relationship ) );
    }

    public void add( PropertyRecord before, PropertyRecord property ) throws IOException
    {
        write( new Command.PropertyCommand( null, before, property ) );
    }

    public void add( RelationshipTypeTokenRecord record ) throws IOException
    {
        write( new Command.RelationshipTypeTokenCommand( null, record ) );
    }

    public void add( PropertyKeyTokenRecord record ) throws IOException
    {
        write( new Command.PropertyKeyTokenCommand( null, record ) );
    }

    public void add( NeoStoreRecord record ) throws IOException
    {
        write( new Command.NeoStoreCommand( null, record ) );
    }

    private void write( Command command ) throws IOException
    {
        LogIoUtils.writeCommand( buffer, identifier, command );
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
