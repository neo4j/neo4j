/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the relationship store.
 */
public class RelationshipStore extends AbstractStore implements Store, RecordStore<RelationshipRecord>
{
    public static abstract class Configuration
        extends AbstractStore.Configuration
    {
    }
    
    public static final String TYPE_DESCRIPTOR = "RelationshipStore";

    // record header size
    // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
    // first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
    // second_next_rel_id+next_prop_id(int)
    public static final int RECORD_SIZE = 33;

    public RelationshipStore(File fileName, Config configuration, IdGeneratorFactory idGeneratorFactory,
                             WindowPoolFactory windowPoolFactory, FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger)
    {
        super(fileName, configuration, IdType.RELATIONSHIP, idGeneratorFactory,
                windowPoolFactory, fileSystemAbstraction, stringLogger);
    }

    @Override
    public void accept( RecordStore.Processor processor, RelationshipRecord record )
    {
        processor.processRelationship( this, record );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public int getRecordHeaderSize()
    {
        return getRecordSize();
    }

    @Override
    public void close()
    {
        super.close();
    }

    @Override
    public RelationshipRecord getRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            return getRecord( id, window, RecordLoad.NORMAL );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public RelationshipRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new RelationshipRecord( id, -1, -1, -1 );
        }

        try
        {
            return getRecord( id, window, RecordLoad.FORCE );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public RelationshipRecord forceGetRaw( RelationshipRecord record )
    {
        return record;
    }

    @Override
    public RelationshipRecord forceGetRaw( long id )
    {
        return forceGetRecord( id );
    }

    public RelationshipRecord getLightRel( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            // ok to high id
            return null;
        }
        try
        {
            RelationshipRecord record = getRecord( id, window, RecordLoad.CHECK );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public void updateRecord( RelationshipRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window, false );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( RelationshipRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
                OperationType.WRITE );
            try
            {
                updateRecord( record, window, true );
            }
            finally
            {
                releaseWindow( window );
            }
    }

    private void updateRecord( RelationshipRecord record,
        PersistenceWindow window, boolean force )
    {
        long id = record.getId();
        registerIdFromUpdateRecord( id );
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() || force )
        {
            long firstNode = record.getFirstNode();
            short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);

            long secondNode = record.getSecondNode();
            long secondNodeMod = (secondNode & 0x700000000L) >> 4;

            long firstPrevRel = record.getFirstPrevRel();
            long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;

            long firstNextRel = record.getFirstNextRel();
            long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;

            long secondPrevRel = record.getSecondPrevRel();
            long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;

            long secondNextRel = record.getSecondNextRel();
            long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

            long nextProp = record.getNextProp();
            long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            short inUseUnsignedByte = (short)((record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue() | firstNodeMod | nextPropMod);

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            int typeInt = (int)(record.getType() | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) firstNode ).putInt( (int) secondNode )
                .putInt( typeInt ).putInt( (int) firstPrevRel ).putInt( (int) firstNextRel )
                .putInt( (int) secondPrevRel ).putInt( (int) secondNextRel ).putInt( (int) nextProp );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    private RelationshipRecord getRecord( long id, PersistenceWindow window,
        RecordLoad load )
    {
        Buffer buffer = window.getOffsettedBuffer( id );

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            switch ( load )
            {
            case NORMAL:
                throw new InvalidRecordException( "RelationshipRecord[" + id + "] not in use" );
            case CHECK:
                return null;
            }
        }

        long firstNode = buffer.getUnsignedInt();
        long firstNodeMod = (inUseByte & 0xEL) << 31;

        long secondNode = buffer.getUnsignedInt();

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        long typeInt = buffer.getInt();
        long secondNodeMod = (typeInt & 0x70000000L) << 4;
        int type = (int)(typeInt & 0xFFFF);

        RelationshipRecord record = new RelationshipRecord( id,
            longFromIntAndMod( firstNode, firstNodeMod ),
            longFromIntAndMod( secondNode, secondNodeMod ), type );
        record.setInUse( inUse );

        long firstPrevRel = buffer.getUnsignedInt();
        long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
        record.setFirstPrevRel( longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

        long firstNextRel = buffer.getUnsignedInt();
        long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
        record.setFirstNextRel( longFromIntAndMod( firstNextRel, firstNextRelMod ) );

        long secondPrevRel = buffer.getUnsignedInt();
        long secondPrevRelMod = (typeInt & 0x380000L) << 13;
        record.setSecondPrevRel( longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

        long secondNextRel = buffer.getUnsignedInt();
        long secondNextRelMod = (typeInt & 0x70000L) << 16;
        record.setSecondNextRel( longFromIntAndMod( secondNextRel, secondNextRelMod ) );

        long nextProp = buffer.getUnsignedInt();
        long nextPropMod = (inUseByte & 0xF0L) << 28;

        record.setNextProp( longFromIntAndMod( nextProp, nextPropMod ) );
        return record;
    }

//    private RelationshipRecord getFullRecord( long id, PersistenceWindow window )
//    {
//        Buffer buffer = window.getOffsettedBuffer( id );
//        byte inUse = buffer.get();
//        boolean inUseFlag = ((inUse & Record.IN_USE.byteValue()) ==
//            Record.IN_USE.byteValue());
//        RelationshipRecord record = new RelationshipRecord( id,
//            buffer.getInt(), buffer.getInt(), buffer.getInt() );
//        record.setInUse( inUseFlag );
//        record.setFirstPrevRel( buffer.getInt() );
//        record.setFirstNextRel( buffer.getInt() );
//        record.setSecondPrevRel( buffer.getInt() );
//        record.setSecondNextRel( buffer.getInt() );
//        record.setNextProp( buffer.getInt() );
//        return record;
//    }

    public RelationshipRecord getChainRecord( long relId )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( relId, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            // ok to high id
            return null;
        }
        try
        {
//            return getFullRecord( relId, window );
            return getRecord( relId, window, RecordLoad.NORMAL );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( getWindowPoolStats() );
        return list;
    }

}