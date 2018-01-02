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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.LogProvider;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

/**
 * Implementation of the relationship store.
 */
public class RelationshipStore extends AbstractRecordStore<RelationshipRecord>
{
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
            LogProvider logProvider )
    {
        super( fileName, configuration, IdType.RELATIONSHIP, idGeneratorFactory, pageCache, logProvider );
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
        return getRecord( new RelationshipRecord( id ) );
    }

    public RelationshipRecord getRecord( RelationshipRecord record )
    {
        return fillRecord( record.getId(), record, RecordLoad.NORMAL ) ? record : null;
    }

    @Override
    public RelationshipRecord forceGetRecord( long id )
    {
        RelationshipRecord record = new RelationshipRecord( -1 );
        return fillRecord( id, record, RecordLoad.FORCE ) ? record : null;
    }

    public RelationshipRecord getLightRel( long id )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        return fillRecord( id, record, RecordLoad.CHECK ) ? record : null;
    }

    /**
     * @return {@code true} if record successfully loaded and in use.
     * If not in use the return value depends on the {@code loadMode}:
     * <ol>
     * <li>NORMAL: throws {@link InvalidRecordException}</li>
     * <li>CHECK: returns {@code false}</li>
     * <li>FORCE: return {@code true}</li>
     * </ol>
     */
    public boolean fillRecord( long id, RelationshipRecord target, RecordLoad loadMode )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( id ), PF_SHARED_LOCK ) )
        {
            boolean success = false;
            if ( cursor.next() )
            {
                do
                {
                    success = readRecord( id, cursor, target );
                } while ( cursor.shouldRetry() );
            }

            if ( !success )
            {
                if ( loadMode == RecordLoad.NORMAL )
                {
                    throw new InvalidRecordException( "RelationshipRecord[" + id + "] not in use" );
                }
                else if ( loadMode == RecordLoad.CHECK )
                {
                    return false;
                }
            }
            return true;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public boolean inUse( long id )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );

        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            boolean recordIsInUse = false;
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    recordIsInUse = isInUse( cursor.getByte() );
                } while ( cursor.shouldRetry() );
            }
            return recordIsInUse;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void forceUpdateRecord( RelationshipRecord record )
    {
        updateRecord( record, true );
    }

    @Override
    public void updateRecord( RelationshipRecord record )
    {
        updateRecord( record, false );
    }

    private void updateRecord( RelationshipRecord record, boolean force )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( record.getId() ), PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() ) // should always be true
            {
                do
                {
                    updateRecord( record, cursor, force );
                } while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void updateRecord( RelationshipRecord record,
        PageCursor cursor, boolean force )
    {
        long id = record.getId();
        cursor.setOffset( offsetForId( id ) );
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
            freeId( id );
        }
    }

    private boolean readRecord( long id, PageCursor cursor,
        RelationshipRecord record )
    {
        cursor.setOffset( offsetForId( id ) );

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        long inUseByte = cursor.getByte();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

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
        return inUse;
    }

    public boolean fillChainRecord( long id, RelationshipRecord record )
    {
        return fillRecord( id, record, RecordLoad.CHECK );
    }
}
