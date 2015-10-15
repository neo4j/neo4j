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
package org.neo4j.kernel.impl.store.format.current;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

public class PropertyRecordFormat extends BaseRecordFormat<PropertyRecord>
{
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
            + 4/*next*/
            + 4/*prev*/
            + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
         // = 41

    public PropertyRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, 0 );
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }

    @Override
    public void read( PropertyRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
    {
        record.clearPropertyBlocks();

        int offsetAtBeginning = cursor.getOffset();

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = cursor.getByte();
        long prevMod = (modifiers & 0xF0L) << 28;
        long nextMod = (modifiers & 0x0FL) << 32;
        long prevProp = cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();
        record.setPrevProp( BaseRecordFormat.longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( BaseRecordFormat.longFromIntAndMod( nextProp, nextMod ) );

        record.setInUse( false );
        while ( cursor.getOffset() - offsetAtBeginning < RECORD_SIZE )
        {
            PropertyBlock newBlock = getPropertyBlock( cursor );
            if ( newBlock != null )
            {
                record.addPropertyBlock( newBlock );
                record.setInUse( true );
            }
            else
            {
                // We assume that storage is defragged
                break;
            }
        }
    }

    /*
     * It is assumed that the argument does hold a property block - all zeros is
     * a valid (not in use) block, so even if the Bits object has been exhausted a
     * result is returned, that has inUse() return false. Also, the argument is not
     * touched.
     */
    private PropertyBlock getPropertyBlock( PageCursor cursor )
    {
        long header = cursor.getLong();
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            return null;
        }
        PropertyBlock toReturn = new PropertyBlock();
        // toReturn.setInUse( true );
        int numBlocks = type.calculateNumberOfBlocksUsed( header );
        long[] blockData = new long[numBlocks];
        blockData[0] = header; // we already have that
        for ( int i = 1; i < numBlocks; i++ )
        {
            blockData[i] = cursor.getLong();
        }
        toReturn.setValueBlocks( blockData );
        return toReturn;
    }

    @Override
    public void write( PropertyRecord record, PageCursor cursor )
    {
        if ( record.inUse() )
        {
            // Set up the record header
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getPrevProp() & 0xF00000000L) >> 28);
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getNextProp() & 0xF00000000L) >> 32);
            byte modifiers = (byte) (prevModifier | nextModifier);
            /*
             * [pppp,nnnn] previous, next high bits
             */
            cursor.putByte( modifiers );
            cursor.putInt( (int) record.getPrevProp() );
            cursor.putInt( (int) record.getNextProp() );

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            for ( PropertyBlock block : record )
            {
                long[] propBlockValues = block.getValueBlocks();
                for ( long propBlockValue : propBlockValues )
                {
                    cursor.putLong( propBlockValue );
                }

                longsAppended += propBlockValues.length;
            }
            if ( longsAppended < PropertyType.getPayloadSizeLongs() )
            {
                cursor.putLong( 0 );
            }
        }
        else
        {
            // skip over the record header, nothing useful there
            cursor.setOffset( cursor.getOffset() + 9 );
            cursor.putLong( 0 );
        }
    }

    @Override
    public long getNextRecordReference( PropertyRecord record )
    {
        return record.getNextProp();
    }
}
