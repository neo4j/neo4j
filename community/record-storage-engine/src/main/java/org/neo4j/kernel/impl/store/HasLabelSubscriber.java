/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.util.Bits;

/**
 * Used for streaming hasLabel check
 */
class HasLabelSubscriber implements RecordSubscriber<DynamicRecord>
{
    private int requiredBits;
    private int remainingBits;
    private Bits bits;
    private boolean firstRecord = true;
    private boolean found;
    private final int label;
    private final DynamicArrayStore labelStore;
    private final PageCursorTracer cursorTracer;

    HasLabelSubscriber( int label, DynamicArrayStore labelStore, PageCursorTracer cursorTracer )
    {
        this.label = label;
        this.labelStore = labelStore;
        this.cursorTracer = cursorTracer;
    }

    boolean hasLabel()
    {
        return found;
    }

    @Override
    public boolean onRecord( DynamicRecord record )
    {
        if ( !record.inUse() )
        {
            return true;
        }
        labelStore.ensureHeavy( record, cursorTracer );

        if ( firstRecord )
        {
            firstRecord = false;
            return processFirstRecord( record.getData() );
        }
        else
        {
            return processRecord( record.getData() );
        }
    }

    private boolean processFirstRecord( byte[] data )
    {
        assert ShortArray.typeOf( data[0] ) == ShortArray.LONG;
        requiredBits = data[2];
        bits = Bits.bitsFromBytes( data, 3 );
        int totalNumberOfBits = (data.length - 3) * 8;
        int numberOfCompleteLabels = totalNumberOfBits / requiredBits;
        remainingBits = totalNumberOfBits - numberOfCompleteLabels * requiredBits;
        return findLabel( (data.length - 3) * 8 / requiredBits, true );
    }

    private boolean processRecord( byte[] data )
    {
        int totalNumberOfBits = remainingBits + (data.length) * 8;
        int numberOfCompleteLabels = totalNumberOfBits / requiredBits;
        computeBits( data, totalNumberOfBits, numberOfCompleteLabels );
        return findLabel( numberOfCompleteLabels, false );
    }

    private void computeBits( byte[] data, int totalNumberOfBits, int numberOfCompleteLabels )
    {
        if ( remainingBits > 0 )
        {
            Bits newBits = Bits.bits( (int) Math.ceil( (totalNumberOfBits + remainingBits) / 8.0 ) );
            newBits.put( bits.getLong( remainingBits ), remainingBits );
            newBits.put( data, 0, data.length );
            bits = newBits;
        }
        else
        {
            bits = Bits.bitsFromBytes( data );
        }
        remainingBits = totalNumberOfBits - numberOfCompleteLabels * requiredBits;
    }

    private boolean findLabel( int numberOfCompleteLabels, boolean skipFirst )
    {
        for ( int i = 0; i < numberOfCompleteLabels; i++ )
        {
            long foundLabel = bits.getLong( requiredBits );
            //first item of the first record is the node id
            if ( skipFirst && i == 0 )
            {
                continue;
            }
            if ( foundLabel == label )
            {
                found = true;
                return false;
            }
        }
        return true;
    }
}
