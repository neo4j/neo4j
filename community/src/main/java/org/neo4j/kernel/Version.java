/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public enum Version
{
    VERSION;
    private final String title;
    private final String vendor;
    private final String version;
    private final String revision;

    @Override
    public String toString()
    {
        if ( title == null )
        {
            return "Neo4j Kernel, unpackaged version " + getVersion();
        }
        return title + " " + getVersion();
    }

    /**
     * Gets the version of the running neo4j kernel.
     *
     * @return
     */
    public String getVersion()
    {
        if ( version == null )
        {
            return "<unknown>";
        }
        else if ( version.contains( "SNAPSHOT" ) )
        {
            return version + " (revision: " + revision + ")";
        }
        else
        {
            return version;
        }
    }

    /**
     * Returns the build revision of the running neo4j kernel.
     *
     * @return build revision
     */
    public String getRevision()
    {
        return revision;
    }

    private Version()
    {
        Package pkg = getClass().getPackage();
        this.title = pkg.getImplementationTitle();
        this.vendor = pkg.getImplementationVendor();
        this.version = pkg.getImplementationVersion();
        @SuppressWarnings( "hiding" ) String revision = null;
        Manifest manifest = getManifest();
        if ( manifest != null )
        {
            Attributes attr = manifest.getAttributes( pkg.getName().replace(
                    '.', '/' ) );
            if ( attr == null )
            {
                attr = manifest.getMainAttributes();
            }
            if ( attr != null )
            {
                revision = revision(
                        attr.getValue( "Implementation-Revision" ),
                        attr.getValue( "Implementation-Revision-Status" ) );
            }
        }
        this.revision = revision;
    }

    private static String revision( String revision, String status )
    {
        if ( revision != null )
        {
            StringBuilder result = new StringBuilder( revision );
            if ( status != null )
            {
                status = mapOf( status ).get( "status" );
                if ( status != null )
                {
                    result.append( status );
                }
            }
            return result.toString();
        }
        return "";
    }

    private static Map<String, String> mapOf( String string )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( String part : string.split( ";" ) )
        {
            int start = part.indexOf( '=' );
            if ( start == -1 )
            {
                result.put( part, "true" );
            }
            else
            {
                result.put( part.substring( 0, start ),
                        part.substring( start + 1 ) );
            }
        }
        return result;
    }

    private Manifest getManifest()
    {
        try
        {
            return new JarFile( new File( getClass().getProtectionDomain()
                    .getCodeSource().getLocation().toURI() ) ).getManifest();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    public static void main( String[] args )
    {
        System.out.println( VERSION );
    }

    static String get()
    {
        return VERSION.toString();
    }
}
