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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Serializes and deserializes value to/from Payloads.
 */
public class AtomicBroadcastSerializer
{
    private ObjectInputStreamFactory objectInputStreamFactory;
    private ObjectOutputStreamFactory objectOutputStreamFactory;

    public AtomicBroadcastSerializer()
    {
        this( new ObjectStreamFactory(), new ObjectStreamFactory() );
    }

    public AtomicBroadcastSerializer( ObjectInputStreamFactory objectInputStreamFactory,
            ObjectOutputStreamFactory objectOutputStreamFactory )
    {
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
    }

    public Payload broadcast( Object value )
            throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = objectOutputStreamFactory.create( bout );
        oout.writeObject( value );
        oout.close();
        byte[] bytes = bout.toByteArray();
        return new Payload( bytes, bytes.length );
    }

    public Object receive( Payload payload )
            throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream in = new ByteArrayInputStream( payload.getBuf(), 0, payload.getLen() );
        ObjectInputStream oin = objectInputStreamFactory.create( in );
        return oin.readObject();
    }
}
