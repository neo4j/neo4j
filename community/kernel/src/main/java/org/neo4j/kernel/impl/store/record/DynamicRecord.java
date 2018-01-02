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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;


public class DynamicRecord extends Abstract64BitRecord
{
    private static final int MAX_BYTES_IN_TO_STRING = 8, MAX_CHARS_IN_TO_STRING = 16;

    private byte[] data = null;
    private int length;
    private long nextBlock = Record.NO_NEXT_BLOCK.intValue();
    private int type;
    private boolean startRecord = true;

    public static DynamicRecord dynamicRecord( long id, boolean inUse )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse );
        return record;
    }

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
    
    public void setStartRecord( boolean startRecord )
    {
        this.startRecord = startRecord;
    }
    
    public boolean isStartRecord()
    {
        return startRecord;
    }

    public int getType()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public boolean isLight()
    {
        return data == null;
    }

    public void setLength( int length )
    {
        this.length = length;
    }

    @Override
    public void setInUse( boolean inUse )
    {
        super.setInUse( inUse );
        if ( !inUse )
        {
            data = null;
        }
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
                .append( ",used=" ).append(inUse() ).append( "," )
                .append( "light=" ).append( isLight() )
                .append("(" ).append( length ).append( "),type=" );
        PropertyType type = PropertyType.getPropertyType( this.type << 24, true );
        if ( type == null ) buf.append( this.type ); else buf.append( type.name() );
        buf.append( ",data=" );
        if ( data != null )
        {
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
                        if (i != 0) buf.append( ',' );
                        buf.append( data[i] );
                    }
                }
                else
                {
                    buf.append( "size=" ).append( data.length );
                }
                buf.append( "]," );
            }
        }
        else
        {
            buf.append( "null," );
        }
        buf.append( "start=" ).append( startRecord );
        buf.append( ",next=" ).append( nextBlock ).append( "]" );
        return buf.toString();
    }
    
    @Override
    public DynamicRecord clone()
    {
        DynamicRecord result = new DynamicRecord( getLongId() );
        if ( data != null )
            result.data = data.clone();
        result.setInUse( inUse() );
        result.length = length;
        result.nextBlock = nextBlock;
        result.type = type;
        result.startRecord = startRecord;
        return result;
    }
    
    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof DynamicRecord ) )
            return false;
        return ((DynamicRecord) obj).getId() == getId();
    }
    
    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }
}
