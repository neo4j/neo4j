/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

public class FileHeader
{
    private final String fileName;
    private final int requiredAlignment;

    public FileHeader( String fileName )
    {
        // A required alignment of 1 basically means that any alignment will do.
        this( fileName, 1 );
    }

    public FileHeader( String fileName, int requiredAlignment )
    {
        this.fileName = fileName;
        this.requiredAlignment = requiredAlignment;
    }

    public String fileName()
    {
        return fileName;
    }

    public int requiredAlignment()
    {
        return requiredAlignment;
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
        FileHeader that = (FileHeader) o;
        return requiredAlignment == that.requiredAlignment && Objects.equals( fileName, that.fileName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileName, requiredAlignment );
    }

    @Override
    public String toString()
    {
        return "FileHeader{" + "fileName='" + fileName + '\'' + ", requiredAlignment=" + requiredAlignment + '}';
    }
}
