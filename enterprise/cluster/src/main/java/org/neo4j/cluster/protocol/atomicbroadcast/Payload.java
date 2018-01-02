/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * AtomicBroadcast payload. Wraps a byte buffer and its length.
 */
public class Payload
        implements Externalizable
{
    private byte[] buf;
    private int len;

    /**
     * Externalizable constructor
     */
    public Payload()
    {
    }

    public Payload( byte[] buf, int len )
    {
        this.buf = buf;
        this.len = len;
    }

    public byte[] getBuf()
    {
        return buf;
    }

    public int getLen()
    {
        return len;
    }

    @Override
    public void writeExternal( ObjectOutput out )
            throws IOException
    {
        // NOTE: This was changed from writing only a byte in 2.2, which doesn't work
        out.writeInt( len );
        out.write( buf, 0, len );
    }

    @Override
    public void readExternal( ObjectInput in )
            throws IOException, ClassNotFoundException
    {
        // NOTE: This was changed from reading only a byte in 2.2, which doesn't work
        len = in.readInt();
        buf = new byte[len];
        in.read( buf, 0, len );
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

        Payload payload = (Payload) o;

        if ( len != payload.len )
        {
            return false;
        }
        if ( !Arrays.equals( buf, payload.buf ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = buf != null ? Arrays.hashCode( buf ) : 0;
        result = 31 * result + len;
        return result;
    }
}
