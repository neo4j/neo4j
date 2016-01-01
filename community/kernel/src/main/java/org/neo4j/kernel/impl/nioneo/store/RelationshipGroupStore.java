/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class RelationshipGroupStore extends AbstractRecordStore<RelationshipGroupRecord> implements Store
{
    /* Record layout
     *
     * [type+inUse+highbits,next,firstOut,firstIn,firstLoop,owningNode] = 25B
     *
     * One record holds first relationship links (out,in,loop) to relationships for one type for one entity.
     */
    public static final int RECORD_SIZE = 25;
    public static final String TYPE_DESCRIPTOR = "RelationshipGroupStore";

    private int denseNodeThreshold;

    public RelationshipGroupStore( File fileName, Config config, IdGeneratorFactory idGeneratorFactory,
            WindowPoolFactory windowPoolFactory, FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger, StoreVersionMismatchHandler versionMismatchHandler )
    {
        super( fileName, config, IdType.RELATIONSHIP_GROUP, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, versionMismatchHandler );
    }

    @Override
    public RelationshipGroupRecord getRecord( long id )
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
    public int getNumberOfReservedLowIds()
    {
        return 1;
    }

    @Override
    protected void readAndVerifyBlockSize() throws IOException
    {
        // Read dense node threshold from the first record in the store (reserved for this threshold)
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        getFileChannel().position( 0 );
        getFileChannel().read( buffer );
        buffer.flip();
        denseNodeThreshold = buffer.getInt();
        if ( denseNodeThreshold < 0 )
        {
            throw new InvalidRecordException( "Illegal block size: " + denseNodeThreshold
                    + " in " + getStorageFileName() );
        }
    }

    private RelationshipGroupRecord getRecord( long id, PersistenceWindow window, RecordLoad load )
    {
        Buffer buffer = window.getOffsettedBuffer( id );

        // [    ,   x] in use
        // [    ,xxx ] high next id bits
        // [ xxx,    ] high firstOut bits
        long inUseByte = buffer.get();
        boolean inUse = (inUseByte&0x1) > 0;
        if ( !inUse )
        {
            switch ( load )
            {
            case NORMAL: throw new InvalidRecordException( "Record[" + id + "] not in use" );
            case CHECK: return null;
            }
        }

        // [    ,xxx ] high firstIn bits
        // [ xxx,    ] high firstLoop bits
        long highByte = buffer.get();

        int type = buffer.getShort();
        long nextLowBits = buffer.getUnsignedInt();
        long nextOutLowBits = buffer.getUnsignedInt();
        long nextInLowBits = buffer.getUnsignedInt();
        long nextLoopLowBits = buffer.getUnsignedInt();
        long owningNode = buffer.getUnsignedInt() | (((long)buffer.get()) << 32);

        long nextMod = (inUseByte & 0xE) << 31;
        long nextOutMod = (inUseByte & 0x70) << 28;
        long nextInMod = (highByte & 0xE) << 31;
        long nextLoopMod = (highByte & 0x70) << 28;

        RelationshipGroupRecord record = new RelationshipGroupRecord( id, type );
        record.setInUse( inUse );
        record.setNext( longFromIntAndMod( nextLowBits, nextMod ) );
        record.setFirstOut( longFromIntAndMod( nextOutLowBits, nextOutMod ) );
        record.setFirstIn( longFromIntAndMod( nextInLowBits, nextInMod ) );
        record.setFirstLoop( longFromIntAndMod( nextLoopLowBits, nextLoopMod ) );
        record.setOwningNode( owningNode );
        return record;
    }

    @Override
    public void updateRecord( RelationshipGroupRecord record )
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

    public void updateRecord( RelationshipGroupRecord record, boolean recovered )
    {
        assert recovered;
        setRecovered();
        try
        {
            updateRecord( record );
            registerIdFromUpdateRecord( record.getId() );
        }
        finally
        {
            unsetRecovered();
        }
    }

    private void updateRecord( RelationshipGroupRecord record, PersistenceWindow window, boolean force )
    {
        long id = record.getId();
        registerIdFromUpdateRecord( id );
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() || force )
        {
            long nextMod = record.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getNext() & 0x700000000L) >> 31;
            long nextOutMod = record.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstOut() & 0x700000000L) >> 28;
            long nextInMod = record.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstIn() & 0x700000000L) >> 31;
            long nextLoopMod = record.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstLoop() & 0x700000000L) >> 28;

            buffer
                // [    ,   x] in use
                // [    ,xxx ] high next id bits
                // [ xxx,    ] high firstOut bits
                .put( (byte) (nextOutMod | nextMod | 1) )

                // [    ,xxx ] high firstIn bits
                // [ xxx,    ] high firstLoop bits
                .put( (byte) (nextLoopMod | nextInMod) )

                .putShort( (short) record.getType() )
                .putInt( (int) record.getNext() )
                .putInt( (int) record.getFirstOut() )
                .putInt( (int) record.getFirstIn() )
                .putInt( (int) record.getFirstLoop() )
                .putInt( (int) record.getOwningNode() ).put( (byte) (record.getOwningNode() >> 32) );
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

    @Override
    public RelationshipGroupRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new RelationshipGroupRecord( id, -1 );
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
    public RelationshipGroupRecord forceGetRaw( long id )
    {
        return forceGetRecord( id );
    }

    @Override
    public void forceUpdateRecord( RelationshipGroupRecord record )
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

    @Override
    public RelationshipGroupRecord forceGetRaw( RelationshipGroupRecord record )
    {
        return record;
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipGroupRecord record )
            throws FAILURE
    {
        processor.processRelationshipGroup( this, record );
    }

    @Override
    public int getRecordHeaderSize()
    {
        return getRecordSize();
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<>();
        list.add( getWindowPoolStats() );
        return list;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    public int getDenseNodeThreshold()
    {
        return denseNodeThreshold;
    }
}
