/*
 * Copyright (c) "Neo4j"
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

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.storageengine.api.Mask;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

public class DynamicRecord extends AbstractBaseRecord
{
    public static final long SHALLOW_SIZE = shallowSizeOfInstance( DynamicRecord.class );
    public static final byte[] NO_DATA = new byte[0];
    private static final int MAX_BYTES_IN_TO_STRING = 8;
    private static final int MAX_CHARS_IN_TO_STRING = 16;

    private byte[] data;
    private long nextBlock;
    private int type;
    private boolean startRecord;

    public DynamicRecord( DynamicRecord other )
    {
        super( other );
        this.data = Arrays.copyOf( other.data, other.data.length );
        this.nextBlock = other.nextBlock;
        this.type = other.type;
        this.startRecord = other.startRecord;
    }

    public DynamicRecord( long id )
    {
        super( id );
    }

    public DynamicRecord initialize( boolean inUse, boolean isStartRecord, long nextBlock, int type )
    {
        super.initialize( inUse );
        this.startRecord = isStartRecord;
        this.nextBlock = nextBlock;
        this.type = type;
        this.data = NO_DATA;
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, true, Record.NO_NEXT_BLOCK.intValue(), -1 );
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
        return PropertyType.getPropertyTypeOrNull( this.type << 24 );
    }

    /**
     * @return The {@link #type} field of this record, as set by previous invocations to {@link #setType(int)} or
     * {@link #initialize(boolean, boolean, long, int)}
     */
    public int getTypeAsInt()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public void setInUse( boolean inUse, int type )
    {
        this.type = type;
        this.setInUse( inUse );
    }

    public void setData( byte[] data )
    {
        this.data = data;
    }

    public int getLength()
    {
        return data.length;
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
    public String toString( Mask mask )
    {
        StringBuilder buf = new StringBuilder();
        buf.append( "DynamicRecord[" )
           .append( getId() )
           .append( ",used=" ).append( inUse() ).append( ',' )
           .append( '(' ).append( mask.filter( data.length ) ).append( "),type=" );
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
        mask.build( buf, this::buildDataString );
        buf.append( ",start=" ).append( startRecord );
        buf.append( ",next=" ).append( nextBlock ).append( ']' );
        return buf.toString();
    }

    private void buildDataString( StringBuilder buf )
    {
        if ( getType() == PropertyType.STRING && data.length <= MAX_CHARS_IN_TO_STRING )
        {
            buf.append( '"' );
            buf.append( PropertyStore.decodeString( data ) );
            buf.append( "\"" );
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
            buf.append( "]" );
        }
    }

    @Override
    public DynamicRecord copy()
    {
        return new DynamicRecord( this );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( super.hashCode(), nextBlock, type, startRecord );
        result = 31 * result + Arrays.hashCode( data );
        return result;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        DynamicRecord that = (DynamicRecord) o;
        return nextBlock == that.nextBlock &&
                type == that.type &&
                startRecord == that.startRecord &&
                Arrays.equals( data, that.data );
    }
}
