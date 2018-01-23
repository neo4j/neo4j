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
package org.neo4j.io.mem;

public class NativeMemoryAllocationRefusedError extends OutOfMemoryError
{
    private final long attemptedAllocationSizeBytes;
    private boolean hasAlreadyAllocatedBytes;
    private long alreadyAllocatedBytes;

    public NativeMemoryAllocationRefusedError( long size )
    {
        this.attemptedAllocationSizeBytes = size;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        StringBuilder sb = new StringBuilder();
        sb.append( "Failed to allocate " ).append( attemptedAllocationSizeBytes ).append( " bytes. " );
        if ( hasAlreadyAllocatedBytes )
        {
            sb.append( "So far " ).append( alreadyAllocatedBytes );
            sb.append( " bytes have already been successfully allocated. " );
        }
        sb.append( "The allocation was refused by the operating system" );
        if ( message != null )
        {
            sb.append( ": " ).append( message );
        }
        else
        {
            sb.append( '.' );
        }
        return sb.toString();
    }

    public void setAlreadyAllocatedBytes( long alreadyAllocatedBytes )
    {
        hasAlreadyAllocatedBytes = true;
        this.alreadyAllocatedBytes = alreadyAllocatedBytes;
    }
}
