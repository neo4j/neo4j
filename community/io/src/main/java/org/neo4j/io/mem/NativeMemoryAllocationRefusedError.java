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

import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;
import static org.neo4j.io.os.OsBeanUtil.getCommittedVirtualMemory;
import static org.neo4j.io.os.OsBeanUtil.getFreePhysicalMemory;
import static org.neo4j.io.os.OsBeanUtil.getTotalPhysicalMemory;

public class NativeMemoryAllocationRefusedError extends OutOfMemoryError
{
    private final long attemptedAllocationSizeBytes;
    private final long alreadyAllocatedBytes;

    NativeMemoryAllocationRefusedError( long size, long alreadyAllocatedBytes )
    {
        this.attemptedAllocationSizeBytes = size;
        this.alreadyAllocatedBytes = alreadyAllocatedBytes;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        StringBuilder sb = new StringBuilder();
        sb.append( "Failed to allocate " ).append( attemptedAllocationSizeBytes ).append( " bytes. " );
        sb.append( "So far " ).append( alreadyAllocatedBytes );
        sb.append( " bytes have already been successfully allocated. " );
        sb.append( "The system currently has " );
        appendBytes( sb, getTotalPhysicalMemory() ).append( " total physical memory, " );
        appendBytes( sb, getCommittedVirtualMemory() ).append( " committed virtual memory, and " );
        appendBytes( sb, getFreePhysicalMemory() ).append( " free physical memory. " );

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

    private StringBuilder appendBytes( StringBuilder sb, long bytes )
    {
        if ( bytes == VALUE_UNAVAILABLE )
        {
            sb.append( "(?) bytes" );
        }
        else
        {
            sb.append( bytes ).append( " bytes" );
        }
        return sb;
    }
}
