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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;

public class LenientObjectOutputStream extends ObjectOutputStream
{
    private VersionMapper versionMapper;

    public LenientObjectOutputStream( ByteArrayOutputStream bout, VersionMapper versionMapper ) throws IOException
    {
        super( bout );
        this.versionMapper = versionMapper;
    }

    @Override
    protected void writeClassDescriptor( ObjectStreamClass desc ) throws IOException
    {
        if ( versionMapper.hasMappingFor( desc.getName() ) )
        {
            updateWirePayloadSuid( desc );
        }

        super.writeClassDescriptor( desc );
    }

    private void updateWirePayloadSuid( ObjectStreamClass wirePayload )
    {
        try
        {
            Field field = getAccessibleSuidField( wirePayload );
            field.set( wirePayload, versionMapper.mappingFor( wirePayload.getName() ) );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Field getAccessibleSuidField( ObjectStreamClass localClassDescriptor ) throws NoSuchFieldException
    {
        Field suidField = localClassDescriptor.getClass().getDeclaredField( "suid" );
        suidField.setAccessible( true );
        return suidField;
    }
}
