/*
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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.LogProvider;

public class RelationshipGroupStore extends CommonAbstractStore<RelationshipGroupRecord>
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

    public RelationshipGroupStore(
            File fileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider )
    {
        super( fileName, config, IdType.RELATIONSHIP_GROUP, idGeneratorFactory, pageCache,
                logProvider, TYPE_DESCRIPTOR );
        denseNodeThreshold = configuration.get( GraphDatabaseSettings.dense_node_threshold );
    }

    @Override
    public int getNumberOfReservedLowIds()
    {
        return 1;
    }

    @Override
    protected ByteBuffer createHeaderRecord()
    {
        return intHeaderData( denseNodeThreshold );
    }

    @Override
    protected void readAndVerifyHeaderRecord() throws IOException
    {
        denseNodeThreshold = getHeaderRecord();
    }

    @Override
    public RelationshipGroupRecord newRecord()
    {
        return new RelationshipGroupRecord( -1 );
    }

    @Override
    protected void readRecord( PageCursor cursor, RelationshipGroupRecord record, RecordLoad mode )
    {
        // [    ,   x] in use
        // [    ,xxx ] high next id bits
        // [ xxx,    ] high firstOut bits
        long inUseByte = cursor.getByte();
        boolean inUse = (inUseByte&0x1) > 0;
        if ( mode.shouldLoad( inUse ) )
        {
            // [    ,xxx ] high firstIn bits
            // [ xxx,    ] high firstLoop bits
            long highByte = cursor.getByte();

            int type = cursor.getShort();
            long nextLowBits = cursor.getUnsignedInt();
            long nextOutLowBits = cursor.getUnsignedInt();
            long nextInLowBits = cursor.getUnsignedInt();
            long nextLoopLowBits = cursor.getUnsignedInt();
            long owningNode = cursor.getUnsignedInt() | (((long)cursor.getByte()) << 32);

            long nextMod = (inUseByte & 0xE) << 31;
            long nextOutMod = (inUseByte & 0x70) << 28;
            long nextInMod = (highByte & 0xE) << 31;
            long nextLoopMod = (highByte & 0x70) << 28;

            record.initialize( inUse, type,
                    longFromIntAndMod( nextOutLowBits, nextOutMod ),
                    longFromIntAndMod( nextInLowBits, nextInMod ),
                    longFromIntAndMod( nextLoopLowBits, nextLoopMod ),
                    owningNode,
                    longFromIntAndMod( nextLowBits, nextMod ) );
        }
    }

    @Override
    protected void writeRecord( PageCursor cursor, RelationshipGroupRecord record )
    {
        long id = record.getId();
        cursor.setOffset( offsetForId( id ) );
        if ( record.inUse() )
        {
            long nextMod = record.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getNext() & 0x700000000L) >> 31;
            long nextOutMod = record.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstOut() & 0x700000000L) >> 28;
            long nextInMod = record.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstIn() & 0x700000000L) >> 31;
            long nextLoopMod = record.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstLoop() & 0x700000000L) >> 28;

            // [    ,   x] in use
            // [    ,xxx ] high next id bits
            // [ xxx,    ] high firstOut bits
            cursor.putByte( (byte) (nextOutMod | nextMod | 1) );

            // [    ,xxx ] high firstIn bits
            // [ xxx,    ] high firstLoop bits
            cursor.putByte( (byte) (nextLoopMod | nextInMod) );

            cursor.putShort( (short) record.getType() );
            cursor.putInt( (int) record.getNext() );
            cursor.putInt( (int) record.getFirstOut() );
            cursor.putInt( (int) record.getFirstIn() );
            cursor.putInt( (int) record.getFirstLoop() );
            cursor.putInt( (int) record.getOwningNode() );
            cursor.putByte( (byte) (record.getOwningNode() >> 32) );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
        }
    }

    @Override
    public long getNextRecordReference( RelationshipGroupRecord record )
    {
        return record.getNext();
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipGroupRecord record )
            throws FAILURE
    {
        processor.processRelationshipGroup( this, record );
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    public int getDenseNodeThreshold()
    {
        return denseNodeThreshold;
    }
}
