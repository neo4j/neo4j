/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

public class DynamicRecord extends AbstractRecord
{
    private byte[] data = null;
    private char[] charData = null;
    private int length;
    private int prevBlock = Record.NO_PREV_BLOCK.intValue();
    private int nextBlock = Record.NO_NEXT_BLOCK.intValue();
    private boolean isLight = false;
    private int type;

    public DynamicRecord( int id )
    {
        super( id );
    }

    public int getType()
    {
        return type;
    }

    void setType( int type )
    {
        this.type = type;
    }

    void setIsLight( boolean status )
    {
        this.isLight = status;
    }

    public boolean isLight()
    {
        return isLight;
    }

    public void setLength( int length )
    {
        this.length = length;
    }

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
        isLight = false;
        this.length = data.length;
        this.data = data;
    }

    public void setCharData( char[] data )
    {
        isLight = false;
        this.length = data.length * 2;
        this.charData = data;
    }

    public int getLength()
    {
        return length;
    }

    public byte[] getData()
    {
        assert !isLight;
        assert charData == null;
        return data;
    }

    public boolean isCharData()
    {
        return charData != null;
    }

    public char[] getDataAsChar()
    {
        assert !isLight;
        assert data == null;
        return charData;
    }

    public int getPrevBlock()
    {
        return prevBlock;
    }

    public void setPrevBlock( int prevBlock )
    {
        this.prevBlock = prevBlock;
    }

    public int getNextBlock()
    {
        return nextBlock;
    }

    public void setNextBlock( int nextBlock )
    {
        this.nextBlock = nextBlock;
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "DynamicRecord[" ).append( getId() ).append( "," ).append(
            inUse() );
        if ( inUse() )
        {
            buf.append( "," ).append( prevBlock ).append( "," ).append(
                isLight ? null : data.length ).append( "," ).append( nextBlock )
                .append( "]" );
        }
        return buf.toString();
    }
}