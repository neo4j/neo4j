/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.logging.Logger;

import org.neo4j.impl.nioneo.store.DynamicRecord;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeRecord;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.PropertyType;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.store.RelationshipRecord;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.transaction.xaframework.LogBuffer;
import org.neo4j.impl.transaction.xaframework.XaCommand;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
abstract class Command extends XaCommand
{
    static Logger logger = Logger.getLogger( Command.class.getName() );

    private final int key;

    Command( int key )
    {
        this.key = key;
    }

    @Override
    protected void setRecovered()
    {
        super.setRecovered();
    }

    int getKey()
    {
        return key;
    }

    public int hashCode()
    {
        return key;
    }

    static void writeDynamicRecord( LogBuffer buffer, DynamicRecord record )
        throws IOException
    {
        // id+type+in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            buffer.putInt( record.getId() ).putInt( record.getType() ).put(
                inUse ).putInt( record.getPrevBlock() ).putInt(
                record.getLength() ).putInt( record.getNextBlock() );
            if ( !record.isLight() )
            {
                if ( !record.isCharData() )
                {
                    byte[] data = record.getData();
                    buffer.put( data );
                }
                else
                {
                    char[] chars = record.getDataAsChar();
                    buffer.put( chars );
                }
            }
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            buffer.putInt( record.getId() ).putInt( record.getType() ).put(
                inUse );
        }
    }

    static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        // id+type+in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
        buffer.clear();
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        int type = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == Record.IN_USE.byteValue() )
        {
            inUse = true;
            buffer.clear();
            buffer.limit( 12 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            record.setPrevBlock( buffer.getInt() );
            int nrOfBytes = buffer.getInt();
            record.setNextBlock( buffer.getInt() );
            buffer.clear();
            buffer.limit( nrOfBytes );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            byte data[] = new byte[nrOfBytes];
            buffer.get( data );
            record.setData( data );
        }
        return record;
    }

    private static final byte NODE_COMMAND = (byte) 1;
    private static final byte PROP_COMMAND = (byte) 2;
    private static final byte REL_COMMAND = (byte) 3;
    private static final byte REL_TYPE_COMMAND = (byte) 4;
    private static final byte PROP_INDEX_COMMAND = (byte) 5;

    static class NodeCommand extends Command
    {
        private final NodeRecord record;
        private final NodeStore store;

        NodeCommand( NodeStore store, NodeRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        public String toString()
        {
            return "NodeCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( NODE_COMMAND );
            buffer.putInt( record.getId() );
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putInt( record.getNextRel() ).putInt(
                    record.getNextProp() );
            }
        }

        static Command readCommand( NeoStore neoStore, 
            ReadableByteChannel byteChannel, ByteBuffer buffer ) 
            throws IOException
        {
            buffer.clear();
            buffer.limit( 5 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( inUseFlag == Record.IN_USE.byteValue() )
            {
                inUse = true;
            }
            else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            NodeRecord record = new NodeRecord( id );
            record.setInUse( inUse );
            if ( inUse )
            {
                buffer.clear();
                buffer.limit( 8 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                record.setNextRel( buffer.getInt() );
                record.setNextProp( buffer.getInt() );
            }
            return new NodeCommand( neoStore.getNodeStore(), record );
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof NodeCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class RelationshipCommand extends Command
    {
        private final RelationshipRecord record;
        private final RelationshipStore store;

        RelationshipCommand( RelationshipStore store, RelationshipRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "RelationshipCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_COMMAND );
            buffer.putInt( record.getId() );
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putInt( record.getFirstNode() ).putInt(
                    record.getSecondNode() ).putInt( record.getType() ).putInt(
                    record.getFirstPrevRel() )
                    .putInt( record.getFirstNextRel() ).putInt(
                        record.getSecondPrevRel() ).putInt(
                        record.getSecondNextRel() ).putInt(
                        record.getNextProp() );
            }
        }
        
        static Command readCommand( NeoStore neoStore, 
            ReadableByteChannel byteChannel, ByteBuffer buffer ) 
            throws IOException
        {
            buffer.clear();
            buffer.limit( 5 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
                .byteValue() )
            {
                inUse = true;
            }
            else if ( (inUseFlag & Record.IN_USE.byteValue()) != Record.NOT_IN_USE
                .byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            RelationshipRecord record;
            if ( inUse )
            {
                buffer.clear();
                buffer.limit( 32 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                record = new RelationshipRecord( id, buffer.getInt(), buffer
                    .getInt(), buffer.getInt() );
                record.setInUse( inUse );
                record.setFirstPrevRel( buffer.getInt() );
                record.setFirstNextRel( buffer.getInt() );
                record.setSecondPrevRel( buffer.getInt() );
                record.setSecondNextRel( buffer.getInt() );
                record.setNextProp( buffer.getInt() );
            }
            else
            {
                record = new RelationshipRecord( id, -1, -1, -1 );
                record.setInUse( false );
            }
            return new RelationshipCommand( neoStore.getRelationshipStore(),
                record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof RelationshipCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class PropertyIndexCommand extends Command
    {
        private final PropertyIndexRecord record;
        private final PropertyIndexStore store;

        PropertyIndexCommand( PropertyIndexStore store,
            PropertyIndexRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "PropertyIndexCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( PROP_INDEX_COMMAND );
            buffer.putInt( record.getId() );
            buffer.put( inUse );
            buffer.putInt( record.getPropertyCount() ).putInt(
                record.getKeyBlockId() );
            if ( record.isLight() )
            {
                buffer.putInt( 0 );
            }
            else
            {
                Collection<DynamicRecord> keyRecords = record.getKeyRecords();
                buffer.putInt( keyRecords.size() );
                for ( DynamicRecord keyRecord : keyRecords )
                {
                    writeDynamicRecord( buffer, keyRecord );
                }
            }
        }

        static Command readCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
            buffer.clear();
            buffer.limit( 17 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
                .byteValue() )
            {
                inUse = true;
            }
            else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            PropertyIndexRecord record = new PropertyIndexRecord( id );
            record.setInUse( inUse );
            record.setPropertyCount( buffer.getInt() );
            record.setKeyBlockId( buffer.getInt() );
            int nrKeyRecords = buffer.getInt();
            for ( int i = 0; i < nrKeyRecords; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                record.addKeyRecord( dr );
            }
            return new PropertyIndexCommand( neoStore.getPropertyStore()
                .getIndexStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof PropertyCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class PropertyCommand extends Command
    {
        private final PropertyRecord record;
        private final PropertyStore store;

        PropertyCommand( PropertyStore store, PropertyRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "PropertyCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(int)+next_prop_id(int)+nr_value_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( PROP_COMMAND );
            buffer.putInt( record.getId() );
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putInt( record.getType().intValue() ).putInt(
                    record.getKeyIndexId() ).putLong( record.getPropBlock() )
                    .putInt( record.getPrevProp() ).putInt(
                        record.getNextProp() );
            }
            if ( record.isLight() )
            {
                buffer.putInt( 0 );
            }
            else
            {
                Collection<DynamicRecord> valueRecords = record
                    .getValueRecords();
                buffer.putInt( valueRecords.size() );
                for ( DynamicRecord valueRecord : valueRecords )
                {
                    writeDynamicRecord( buffer, valueRecord );
                }
            }
        }

        static Command readCommand( NeoStore neoStore, 
            ReadableByteChannel byteChannel, ByteBuffer buffer ) 
            throws IOException
        {
            // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(int)+next_prop_id(int)+nr_value_records(int)
            buffer.clear();
            buffer.limit( 5 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
                .byteValue() )
            {
                inUse = true;
            }
            else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            PropertyRecord record = new PropertyRecord( id );
            if ( inUse )
            {
                buffer.clear();
                buffer.limit( 24 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                PropertyType type = getType( buffer.getInt() );
                record.setType( type );
                record.setInUse( inUse );
                record.setKeyIndexId( buffer.getInt() );
                record.setPropBlock( buffer.getLong() );
                record.setPrevProp( buffer.getInt() );
                record.setNextProp( buffer.getInt() );
            }
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int nrValueRecords = buffer.getInt();
            for ( int i = 0; i < nrValueRecords; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                record.addValueRecord( dr );
            }
            return new PropertyCommand( neoStore.getPropertyStore(), record );
        }

        private static PropertyType getType( int type )
        {
            switch ( type )
            {
                case 1:
                    return PropertyType.INT;
                case 2:
                    return PropertyType.STRING;
                case 3:
                    return PropertyType.BOOL;
                case 4:
                    return PropertyType.DOUBLE;
                case 5:
                    return PropertyType.FLOAT;
                case 6:
                    return PropertyType.LONG;
                case 7:
                    return PropertyType.BYTE;
                case 8:
                    return PropertyType.CHAR;
                case 9:
                    return PropertyType.ARRAY;
                case 10:
                    return PropertyType.SHORT;
            }
            throw new RuntimeException( "Unkown property type:" + type );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof PropertyCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class RelationshipTypeCommand extends Command
    {
        private final RelationshipTypeRecord record;
        private final RelationshipTypeStore store;

        RelationshipTypeCommand( RelationshipTypeStore store,
            RelationshipTypeRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "RelationshipTypeCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_TYPE_COMMAND );
            buffer.putInt( record.getId() ).put( inUse ).putInt(
                record.getTypeBlock() );

            Collection<DynamicRecord> typeRecords = record.getTypeRecords();
            buffer.putInt( typeRecords.size() );
            for ( DynamicRecord typeRecord : typeRecords )
            {
                writeDynamicRecord( buffer, typeRecord );
            }
        }

        static Command readCommand( NeoStore neoStore, 
            ReadableByteChannel byteChannel, ByteBuffer buffer ) 
            throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            buffer.clear();
            buffer.limit( 13 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( (inUseFlag & Record.IN_USE.byteValue()) == 
                Record.IN_USE.byteValue() )
            {
                inUse = true;
            }
            else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            RelationshipTypeRecord record = new RelationshipTypeRecord( id );
            record.setInUse( inUse );
            record.setTypeBlock( buffer.getInt() );
            int nrTypeRecords = buffer.getInt();
            for ( int i = 0; i < nrTypeRecords; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                record.addTypeRecord( dr );
            }
            return new RelationshipTypeCommand( 
                neoStore.getRelationshipTypeStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof RelationshipTypeCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static Command readCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return NodeCommand.readCommand( neoStore, byteChannel, buffer );
            case PROP_COMMAND:
                return PropertyCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case PROP_INDEX_COMMAND:
                return PropertyIndexCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case REL_COMMAND:
                return RelationshipCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case REL_TYPE_COMMAND:
                return RelationshipTypeCommand.readCommand( neoStore,
                    byteChannel, buffer );
            default:
                throw new IOException( "Unkown command type[" + commandType
                    + "]" );
        }
    }
}