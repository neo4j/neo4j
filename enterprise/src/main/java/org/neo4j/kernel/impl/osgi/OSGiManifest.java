package org.neo4j.kernel.impl.osgi;

import org.osgi.framework.Bundle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Wraps an OSGi Bundle in a jar Manifest.
 */
public class OSGiManifest extends Manifest
{
    private Bundle bundle; 

    public OSGiManifest( Bundle bundle) {
        this.bundle = bundle;
    }

    @Override
    public Attributes getMainAttributes()
    {
        Attributes mains = new Attributes( );
        mains.putAll( getAttributes("Manifest-Version") );
        mains.putAll( getAttributes("Created-By"));
        mains.putAll( getAttributes("Implementation-Revision"));
        mains.putAll( getAttributes("Implementation-Revision-Status"));
        return mains;
    }

    @Override
    public Map<String, Attributes> getEntries()
    {
        Map<String, Attributes> entries = new HashMap<String, Attributes>();
        Enumeration keys = bundle.getHeaders().keys();
        while (keys.hasMoreElements()) {
            String key = (String)keys.nextElement();
            entries.put(key, getAttributes( key ));
        }
        return entries;
    }

    @Override
    public Attributes getAttributes( String name )
    {
        Attributes requestedAttributes = new Attributes();
        requestedAttributes.put( name, bundle.getHeaders().get( name ));
        return requestedAttributes;
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write( OutputStream out ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read( InputStream is ) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
