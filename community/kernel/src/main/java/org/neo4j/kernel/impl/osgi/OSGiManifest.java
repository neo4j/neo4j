/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.osgi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.osgi.framework.Bundle;

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
