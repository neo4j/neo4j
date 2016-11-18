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
package org.neo4j.kernel.impl.index.labelscan;

public class LabelScanKey
{
    public int labelId = -1;
    public long idRange = -1;

    public LabelScanKey set( int labelId, long idRange )
    {
        this.labelId = labelId;
        this.idRange = idRange;
        return this;
    }

    @Override
    public String toString()
    {
        return "[lbl:" + labelId + ",range:" + idRange + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + labelId;
        result = prime * result + (int) (idRange ^ (idRange >>> 32));
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        LabelScanKey other = (LabelScanKey) obj;
        if ( labelId != other.labelId )
            return false;
        if ( idRange != other.idRange )
            return false;
        return true;
    }
}
