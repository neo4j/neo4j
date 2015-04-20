/*
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
package org.neo4j.kernel.impl.store.format.v2_2;

import java.util.Collections;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.standard.BaseRecordCursor;
import org.neo4j.kernel.impl.store.standard.FixedSizeRecordStoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;

import static org.neo4j.kernel.impl.store.format.NeoStoreFormatUtils.longFromIntAndMod;

/**
 * This defines the full format of the NodeStore, and contains an inner class that defines the record format.
 */
public class NodeStoreFormat_v2_2 extends FixedSizeRecordStoreFormat<NodeRecord, NodeStoreFormat_v2_2.NodeRecordCursor>
{
    private final NodeRecordFormat recordFormat;

    public NodeStoreFormat_v2_2()
    {
        super( NodeRecordFormat.RECORD_SIZE, "NodeStore", CommonAbstractStore.ALL_STORES_VERSION );
        this.recordFormat = new NodeRecordFormat();
    }

    @Override
    public NodeStoreFormat_v2_2.NodeRecordCursor createCursor( PagedFile file, StoreToolkit toolkit, int flags )
    {
        return new NodeRecordCursor( file, toolkit, recordFormat, flags );
    }

    @Override
    public StoreFormat.RecordFormat<NodeRecord> recordFormat()
    {
        return recordFormat;
    }

    /** Full definition of the record format */
    public static class NodeRecordFormat implements StoreFormat.RecordFormat<NodeRecord>
    {
        private final static int IN_USE         = 0;
        private final static int NEXT_REL_BASE  = 1 + IN_USE ;
        private final static int NEXT_PROP_BASE = 4 + NEXT_REL_BASE;
        private final static int LSB_LABELS     = 4 + NEXT_PROP_BASE;
        private final static int MSB_LABELS     = 4 + LSB_LABELS;
        private final static int EXTRA          = 1 + MSB_LABELS;

        private final static int RECORD_SIZE = EXTRA + 1;

        @Override
        public String recordName()
        {
            return "NodeRecord";
        }

        @Override
        public long id( NodeRecord nodeRecord )
        {
            return nodeRecord.getId();
        }

        @Override
        public NodeRecord newRecord( long id )
        {
            return new NodeRecord( id );
        }

        @Override
        public void serialize( PageCursor cursor, int offset, NodeRecord record )
        {
            if(record.inUse())
            {
                long nextRel = record.getNextRel();
                long nextProp = record.getNextProp();

                short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((nextRel & 0x700000000L) >> 31);
                short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short)((nextProp & 0xF00000000L) >> 28);

                // [    ,   x] in use bit
                // [    ,xxx ] higher bits for rel id
                // [xxxx,    ] higher bits for prop id
                short inUseUnsignedByte = ( record.inUse() ? Record.IN_USE : Record.NOT_IN_USE ).byteValue();
                inUseUnsignedByte = (short) ( inUseUnsignedByte | relModifier | propModifier );

                cursor.putByte( offset + IN_USE, (byte) inUseUnsignedByte );
                cursor.putInt(  offset + NEXT_REL_BASE, (int) nextRel );
                cursor.putInt(  offset + NEXT_PROP_BASE, (int) nextProp );

                // lsb of labels
                long labelField = record.getLabelField();
                cursor.putInt( offset + LSB_LABELS, (int) labelField );
                // msb of labels
                cursor.putByte( offset + MSB_LABELS, (byte) ((labelField & 0xFF00000000L) >> 32) );

                byte extra = record.isDense() ? (byte)1 : (byte)0;
                cursor.putByte( offset + EXTRA, extra );
            }
            else
            {
                cursor.putByte( offset + IN_USE, (byte)0);
            }
        }

        @Override
        public void deserialize( PageCursor cursor, int offset, long id, NodeRecord record )
        {
            record.setId( id );
            byte inUseByte = cursor.getByte(offset + IN_USE);
            long nextRel   = cursor.getUnsignedInt( offset + NEXT_REL_BASE );
            long nextProp  = cursor.getUnsignedInt(offset + NEXT_PROP_BASE);
            long lsbLabels = cursor.getUnsignedInt(offset + LSB_LABELS);
            long hsbLabels = cursor.getByte(offset + MSB_LABELS ) & 0xFF; // so that a negative byte won't fill the "extended" bits with ones
            byte extra     = cursor.getByte(offset + EXTRA);

            long relModifier = (inUseByte & 0xEL) << 31;
            long propModifier = (inUseByte & 0xF0L) << 28;
            long labels = lsbLabels | (hsbLabels << 32);
            boolean dense = (extra & 0x1) > 0;

            record.setDense( dense );
            record.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
            record.setNextProp( longFromIntAndMod( nextProp, propModifier ) );
            record.setInUse( (inUseByte & 0x1) == 1 );
            record.setLabelField( labels, Collections.<DynamicRecord>emptyList() );
        }

        @Override
        public boolean inUse( PageCursor cursor, int offset )
        {
            return (cursor.getByte( offset + IN_USE ) & 0x1) == 1;
        }

        public long firstRelationship( PageCursor cursor, int offset )
        {
            return longFromIntAndMod(
                     cursor.getUnsignedInt( offset + NEXT_REL_BASE ),
                    (cursor.getByte(offset + IN_USE) & 0xEL) << 31 );
        }
    }

    /**
     * This is our custom record cursor, extending {@link org.neo4j.kernel.impl.store.standard.BaseRecordCursor} to
     * get required common functionality, but adding some custom field-reading of our own.
     */
    public static class NodeRecordCursor extends BaseRecordCursor<NodeRecord, NodeRecordFormat>
    {
        public NodeRecordCursor( PagedFile file, StoreToolkit toolkit, NodeRecordFormat format, int flags )
        {
            super( file, toolkit, format, flags );
        }

        /** Read the first rel id from the record the cursor currently points at. */
        public long firstRelationship()
        {
            return format.firstRelationship( pageCursor, currentRecordOffset );
        }
    }
}
