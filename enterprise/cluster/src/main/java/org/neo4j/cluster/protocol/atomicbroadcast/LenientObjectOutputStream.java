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
        Long wireSuid = getSuidFrom( desc );

        if ( versionMapper.hasMappingFor( wireSuid ) )
        {
            updateWirePayloadSuid( desc, wireSuid );
        }

        super.writeClassDescriptor( desc );
    }

    private void updateWirePayloadSuid( ObjectStreamClass wirePayload, Long wireSuid )
    {
        try
        {
            Field field = getAccessibleSuidField( wirePayload );
            field.set( wirePayload, versionMapper.map( wireSuid ) );
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

    private Long getSuidFrom( ObjectStreamClass localClassDescriptor )
    {
        try
        {
            Field field = getAccessibleSuidField( localClassDescriptor );
            return (Long) field.get( localClassDescriptor );
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
