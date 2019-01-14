/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;

public class DynamicRecord extends AbstractBaseRecord
{
    public static final byte[] NO_DATA = new byte[0];
    private static final int MAX_BYTES_IN_TO_STRING = 8;
    private static final int MAX_CHARS_IN_TO_STRING = 16;

    private byte[] data;
    private int length;
    private long nextBlock;
    private int type;
    private boolean startRecord;

    /**
     * @deprecated use {@link #initialize(boolean, boolean, long, int, int)} instead.
     */
    @Deprecated
    public static DynamicRecord dynamicRecord( long id, boolean inUse )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse );
        return record;
    }

    /**
     * @deprecated use {@link #initialize(boolean, boolean, long, int, int)} instead.
     */
    @Deprecated
    public static DynamicRecord dynamicRecord( long id, boolean inUse, boolean isStartRecord, long nextBlock, int type,
                                               byte [] data )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse );
        record.setStartRecord( isStartRecord );
        record.setNextBlock( nextBlock );
        record.setType( type );
        record.setData( data );
        return record;
    }

    public DynamicRecord( long id )
    {
        super( id );
    }

    public DynamicRecord initialize( boolean inUse, boolean isStartRecord, long nextBlock,
            int type, int length )
    {
        super.initialize( inUse );
        this.startRecord = isStartRecord;
        this.nextBlock = nextBlock;
        this.type = type;
        this.data = NO_DATA;
        this.length = length;
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, true, Record.NO_NEXT_BLOCK.intValue(), -1, 0 );
    }

    public void setStartRecord( boolean startRecord )
    {
        this.startRecord = startRecord;
    }

    public boolean isStartRecord()
    {
        return startRecord;
    }

    /**
     * @return The {@link PropertyType} of this record or null if unset or non valid
     */
    public PropertyType getType()
    {
        return PropertyType.getPropertyTypeOrNull( (long) (this.type << 24) );
    }

    /**
     * @return The {@link #type} field of this record, as set by previous invocations to {@link #setType(int)} or
     * {@link #initialize(boolean, boolean, long, int, int)}
     */
    public int getTypeAsInt()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public void setLength( int length )
    {
        this.length = length;
    }

    public void setInUse( boolean inUse, int type )
    {
        this.type = type;
        this.setInUse( inUse );
    }

    public void setData( byte[] data )
    {
        this.length = data.length;
        this.data = data;
    }

    public int getLength()
    {
        return length;
    }

    public byte[] getData()
    {
        return data;
    }

    public long getNextBlock()
    {
        return nextBlock;
    }

    public void setNextBlock( long nextBlock )
    {
        this.nextBlock = nextBlock;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append( "DynamicRecord[" )
                .append( getId() )
                .append( ",used=" ).append( inUse() ).append( ',' )
                .append( '(' ).append( length ).append( "),type=" );
        PropertyType type = getType();
        if ( type == null )
        {
            buf.append( this.type );
        }
        else
        {
            buf.append( type.name() );
        }
        buf.append( ",data=" );
        if ( type == PropertyType.STRING && data.length <= MAX_CHARS_IN_TO_STRING )
        {
            buf.append( '"' );
            buf.append( PropertyStore.decodeString( data ) );
            buf.append( "\"," );
        }
        else
        {
            buf.append( "byte[" );
            if ( data.length <= MAX_BYTES_IN_TO_STRING )
            {
                for ( int i = 0; i < data.length; i++ )
                {
                    if ( i != 0 )
                    {
                        buf.append( ',' );
                    }
                    buf.append( data[i] );
                }
            }
            else
            {
                buf.append( "size=" ).append( data.length );
            }
            buf.append( "]," );
        }
        buf.append( "start=" ).append( startRecord );
        buf.append( ",next=" ).append( nextBlock ).append( ']' );
        return buf.toString();
    }

    @Override
    public DynamicRecord clone()
    {
        DynamicRecord clone = new DynamicRecord( getId() ).initialize( inUse(),
                startRecord, nextBlock, type, length );
        if ( data != null )
        {
            clone.setData( data.clone() );
        }
        return clone;
    }
}
