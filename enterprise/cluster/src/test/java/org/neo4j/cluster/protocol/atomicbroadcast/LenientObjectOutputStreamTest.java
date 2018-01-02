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
import java.io.ObjectStreamClass;
import java.net.URI;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.kernel.impl.store.StoreId;

import static junit.framework.Assert.assertEquals;

public class LenientObjectOutputStreamTest
{
    @Test
    public void shouldUseStoredSerialVersionUIDWhenSerialisingAnObject() throws IOException, ClassNotFoundException
    {
        // given
        MemberIsAvailable memberIsAvailable = memberIsAvailable();

        VersionMapper versionMapper = new VersionMapper();
        versionMapper.addMappingFor( memberIsAvailable.getClass().getName(), 12345l );

        // when
        Object deserialisedObject = deserialise( serialise( memberIsAvailable, versionMapper ) );

        // then
        assertEquals( 12345l, serialVersionUIDFor( deserialisedObject ));
    }

    @Test
    public void shouldUseDefaultSerialVersionUIDWhenSerialisingAnObjectifNoMappingExists()
            throws IOException, ClassNotFoundException
    {
        // given
        VersionMapper emptyVersionMapper = new VersionMapper();
        MemberIsAvailable memberIsAvailable = memberIsAvailable();

        // when
        Object deserialisedObject = deserialise( serialise( memberIsAvailable, emptyVersionMapper ) );

        // then
        assertEquals( serialVersionUIDFor( memberIsAvailable ), serialVersionUIDFor( deserialisedObject ));
    }

    private Object deserialise( byte[] bytes ) throws IOException, ClassNotFoundException
    {
        return new ObjectInputStream( inputStreamFor( new Payload( bytes, bytes.length ) ) ).readObject();
    }

    private long serialVersionUIDFor( Object memberIsAvailable )
    {
        return ObjectStreamClass.lookup( memberIsAvailable.getClass() ).getSerialVersionUID();
    }

    private ByteArrayInputStream inputStreamFor( Payload payload )
    {
        return new ByteArrayInputStream( payload.getBuf(), 0, payload.getLen() );
    }

    private byte[] serialise( Object value, VersionMapper versionMapper ) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(  );
        ObjectOutputStream oout = new LenientObjectOutputStream( bout, versionMapper );
        oout.writeObject( value );
        oout.close();
        return bout.toByteArray();
    }

    private MemberIsAvailable memberIsAvailable()
    {
        return new MemberIsAvailable( "r1", new InstanceId( 1 ), URI.create( "http://me" ),
                URI.create( "http://me?something" ), StoreId.DEFAULT );
    }
}
