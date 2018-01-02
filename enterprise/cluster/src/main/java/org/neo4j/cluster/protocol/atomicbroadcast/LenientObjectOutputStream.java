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
        catch ( NoSuchFieldException e )
        {
            throw new RuntimeException( e );
        }
        catch ( IllegalAccessException e )
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
