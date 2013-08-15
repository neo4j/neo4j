package org.neo4j.desktop.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExtensionPackagesConfig implements Value<List<String>>
{
    private final File file;
    private final ListIO io = new FileSystemListIO();
    private final Environment environment;

    public ExtensionPackagesConfig( Environment environment )
    {
        this.environment = environment;
        this.file = new File( environment.getExtensionsDirectory(), "packages" );
    }

    @Override
    public List<String> get()
    {
        try
        {
            return io.read( new ArrayList<String>(), file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void set( List<String> value ) throws IllegalStateException
    {
        try
        {
            environment.getExtensionsDirectory().mkdirs();
            io.write( value, file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
