/**
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
package org.neo4j.kernel.impl.locking;

import static java.lang.String.format;

public final class IndexEntryLock
{
    private final int labelId;
    private final int propertyKeyId;
    private final String propertyValue;

    public IndexEntryLock( int labelId, int propertyKeyId, String propertyValue )
    {
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.propertyValue = propertyValue;
    }

    public int labelId()
    {
        return labelId;
    }

    public int propertyKeyId()
    {
        return propertyKeyId;
    }

    public String propertyValue()
    {
        return propertyValue;
    }

    @Override
    public String toString()
    {
        return format( "IndexEntryLock{labelId=%d, propertyKeyId=%d, propertyValue=%s}",
                       labelId, propertyKeyId, propertyValue );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj instanceof IndexEntryLock )
        {
            IndexEntryLock that = (IndexEntryLock) obj;
            return labelId == that.labelId && propertyKeyId == that.propertyKeyId &&
                   propertyValue.equals( that.propertyValue );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = labelId;
        result = 31 * result + propertyKeyId;
        result = 31 * result + propertyValue.hashCode();
        return result;
    }
}
