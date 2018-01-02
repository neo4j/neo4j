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
 * Stream factory for serializing/deserializing messages.
 */
public class ObjectStreamFactory implements ObjectInputStreamFactory, ObjectOutputStreamFactory
{
    private final VersionMapper versionMapper;

    public ObjectStreamFactory()
    {
        versionMapper = new VersionMapper();
    }

    @Override
    public ObjectOutputStream create( ByteArrayOutputStream bout ) throws IOException
    {
        return new LenientObjectOutputStream( bout, versionMapper );
    }

    @Override
    public ObjectInputStream create( ByteArrayInputStream in ) throws IOException
    {
        return new LenientObjectInputStream( in, versionMapper );
    }
}
