/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Implementation of the relationship store.
 */
public class RelationshipStore extends AbstractRecordStore<RelationshipRecord> implements Store
{
    public static abstract class Configuration
        extends AbstractStore.Configuration
    {
    }

    public static final String TYPE_DESCRIPTOR = "RelationshipStore";

    // record header size
    // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
    // first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
    // second_next_rel_id+next_prop_id(int)+first-in-chain-markers(1)
    public static final int RECORD_SIZE = 34;

    public RelationshipStore(
            File fileName,
            Config configuration,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger,
            StoreVersionMismatchHandler versionMismatchHandler,
            Monitors monitors )
    {
        super( fileName, configuration, IdType.RELATIONSHIP, idGeneratorFactory,
                pageCache, fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipRecord record ) throws FAILURE
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
    public RelationshipRecord getRecord( long id )
    {
        return getRecord( id, new RelationshipRecord( id ) );
    }

    public RelationshipRecord getRecord( long id, RelationshipRecord target )
    {
        return getRecord( id, target, RecordLoad.NORMAL );
    }

    @Override
    public RelationshipRecord forceGetRecord( long id )
    {
        try
        {
            assertIdExists( id );
        }
        catch ( InvalidRecordException e )
        {
            return new RelationshipRecord( id, -1, -1, -1 );
        }
        return getRecord( id, new RelationshipRecord( id ), RecordLoad.FORCE );
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
        try
        {
            assertIdExists( id );
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }

        return getRecord( id, new RelationshipRecord( id ), RecordLoad.CHECK );
    }

    private RelationshipRecord getRecord( long id, RelationshipRecord target, RecordLoad loadMode )
    {
        PageCursor cursor = pageCache.newCursor();
        try
        {
            storeFile.pin( cursor, PageLock.SHARED, pageIdForRecord( id ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        try
        {
            return getRecord( id, cursor, loadMode, target );
        }
        finally
        {
            storeFile.unpin( cursor );
        }
    }

    @Override
    public void updateRecord( RelationshipRecord record )
    {
        PageCursor cursor = pageCache.newCursor();
        try
        {
            storeFile.pin( cursor, PageLock.EXCLUSIVE, pageIdForRecord( record.getId() ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        try
        {
            updateRecord( record, cursor, false );
        }
        finally
        {
            storeFile.unpin( cursor );
        }
    }

    @Override
    public void forceUpdateRecord( RelationshipRecord record )
    {
        PageCursor cursor = pageCache.newCursor();
        try
        {
            storeFile.pin( cursor, PageLock.EXCLUSIVE, pageIdForRecord( record.getId() ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        try
        {
            updateRecord( record, cursor, true );
        }
        finally
        {
            storeFile.unpin( cursor );
        }
    }

    private void updateRecord( RelationshipRecord record,
        PageCursor cursor, boolean force )
    {
        long id = record.getId();
        cursor.setOffset( offsetForId( id ) );
        registerIdFromUpdateRecord( id );
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

            // [    ,   x] 1:st in start node chain, 0x1
            // [    ,  x ] 1:st in end node chain,   0x2
            long firstInStartNodeChain = record.isFirstInFirstChain() ? 0x1 : 0;
            long firstInEndNodeChain = record.isFirstInSecondChain() ? 0x2 : 0;
            byte extraByte = (byte) (firstInEndNodeChain | firstInStartNodeChain);

            cursor.putByte( (byte)inUseUnsignedByte );
            cursor.putInt( (int) firstNode );
            cursor.putInt( (int) secondNode );
            cursor.putInt( typeInt );
            cursor.putInt( (int) firstPrevRel );
            cursor.putInt( (int) firstNextRel );
            cursor.putInt( (int) secondPrevRel );
            cursor.putInt( (int) secondNextRel );
            cursor.putInt( (int) nextProp );
            cursor.putByte( extraByte );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    private RelationshipRecord getRecord( long id, PageCursor cursor,
        RecordLoad load, RelationshipRecord record )
    {
        cursor.setOffset( offsetForId( id ) );

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        long inUseByte = cursor.getByte();

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

        long firstNode = cursor.getUnsignedInt();
        long firstNodeMod = (inUseByte & 0xEL) << 31;

        long secondNode = cursor.getUnsignedInt();

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        long typeInt = cursor.getInt();
        long secondNodeMod = (typeInt & 0x70000000L) << 4;
        int type = (int)(typeInt & 0xFFFF);

        record.setId( id );
        record.setFirstNode( longFromIntAndMod( firstNode, firstNodeMod ) );
        record.setSecondNode( longFromIntAndMod( secondNode, secondNodeMod ) );
        record.setType( type );
        record.setInUse( inUse );

        long firstPrevRel = cursor.getUnsignedInt();
        long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
        record.setFirstPrevRel( longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

        long firstNextRel = cursor.getUnsignedInt();
        long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
        record.setFirstNextRel( longFromIntAndMod( firstNextRel, firstNextRelMod ) );

        long secondPrevRel = cursor.getUnsignedInt();
        long secondPrevRelMod = (typeInt & 0x380000L) << 13;
        record.setSecondPrevRel( longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

        long secondNextRel = cursor.getUnsignedInt();
        long secondNextRelMod = (typeInt & 0x70000L) << 16;
        record.setSecondNextRel( longFromIntAndMod( secondNextRel, secondNextRelMod ) );

        long nextProp = cursor.getUnsignedInt();
        long nextPropMod = (inUseByte & 0xF0L) << 28;

        byte extraByte = cursor.getByte();

        record.setFirstInFirstChain( (extraByte & 0x1) != 0 );
        record.setFirstInSecondChain( (extraByte & 0x2) != 0 );

        record.setNextProp( longFromIntAndMod( nextProp, nextPropMod ) );
        return record;
    }

    public RelationshipRecord getChainRecord( long id )
    {
        try
        {
            assertIdExists( id );
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }

        return getRecord( id, new RelationshipRecord( id ), RecordLoad.NORMAL );
    }
}
