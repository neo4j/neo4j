/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.atomicbroadcast;


import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.kernel.impl.store.StoreId;

import static org.junit.Assert.assertEquals;

public class LenientObjectOutputStreamTest
{
    @Test
    public void shouldUseStoredSerialVersionUIDWhenSerialisingAnObject() throws IOException, ClassNotFoundException
    {
        // given
        MemberIsAvailable memberIsAvailable = memberIsAvailable();

        VersionMapper versionMapper = new VersionMapper();
        versionMapper.addMappingFor( memberIsAvailable.getClass().getName(), 12345L );

        // when
        Object deserialisedObject = deserialise( serialise( memberIsAvailable, versionMapper ) );

        // then
        assertEquals( 12345L, serialVersionUIDFor( deserialisedObject ));
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
