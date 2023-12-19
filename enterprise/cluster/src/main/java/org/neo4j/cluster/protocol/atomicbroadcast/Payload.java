/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
            throws IOException
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
        return Arrays.equals( buf, payload.buf );
    }

    @Override
    public int hashCode()
    {
        int result = buf != null ? Arrays.hashCode( buf ) : 0;
        result = 31 * result + len;
        return result;
    }
}
