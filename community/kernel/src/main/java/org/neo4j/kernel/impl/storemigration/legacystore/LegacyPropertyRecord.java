/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class LegacyPropertyRecord extends Abstract64BitRecord
{
    private LegacyPropertyType type;
    private int keyIndexId = Record.NO_NEXT_BLOCK.intValue();
    private long propBlock = Record.NO_NEXT_BLOCK.intValue();
    private long prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
    private long nextProp = Record.NO_NEXT_PROPERTY.intValue();

    public LegacyPropertyRecord( long id )
    {
        super( id );
    }

    public void setType( LegacyPropertyType type )
    {
        this.type = type;
    }

    public LegacyPropertyType getType()
    {
        return type;
    }

    public int getKeyIndexId()
    {
        return keyIndexId;
    }

    public void setKeyIndexId( int keyId )
    {
        this.keyIndexId = keyId;
    }

    public long getPropBlock()
    {
        return propBlock;
    }

    public void setPropBlock( long propBlock )
    {
        this.propBlock = propBlock;
    }

    public long getPrevProp()
    {
        return prevProp;
    }

    public void setPrevProp( long prevProp )
    {
        this.prevProp = prevProp;
    }

    public long getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( long nextProp )
    {
        this.nextProp = nextProp;
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "LegacyPropertyRecord[" ).append( getId() ).append( "," ).append(
                inUse() ).append( "," ).append( type ).append( "," ).append(
                keyIndexId ).append( "," ).append( propBlock ).append( "," )
                .append( prevProp ).append( "," ).append( nextProp );
        buf.append( "]" );
        return buf.toString();
    }

}
