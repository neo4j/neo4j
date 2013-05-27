/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.ComponentVersion;

public class Version extends Service
{
    protected static final String KERNEL_ARTIFACT_ID = "neo4j-kernel";
    private static final Version KERNEL_VERSION;
    static
    {
        Version kernelVersion;
        try
        {
            kernelVersion = Service.load( Version.class, KERNEL_ARTIFACT_ID );
        }
        catch ( NoSuchElementException ex )
        {
            kernelVersion = null; // Be explicit about what we want.
        }
        if ( kernelVersion == null ) kernelVersion = new Version( KERNEL_ARTIFACT_ID, "" );
        KERNEL_VERSION = kernelVersion;
    }
    private final String title;
    @SuppressWarnings( "unused" )
    private final String vendor;
    private final String version;

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
     * @return the version of the neo4j kernel
     */
    public String getVersion()
    {
        if ( version == null || version.equals( "" ) )
        {
            String revision = getRevision();
            if ( revision == null || revision.equals( "" ) ) return "unknown";
            return "revision: " + getRevision();
        }
        else if ( version.contains( "SNAPSHOT" ) )
        {
            String revision = getRevision();
            if ( revision == null || revision.equals( "" ) ) return version;
            return version + " (revision: " + getRevision() + ")";
        }
        else
        {
            return version;
        }
    }

    public String getReleaseVersion() {
        return version;
    }


    /**
     * Returns the build revision of the running neo4j kernel.
     *
     * @return build revision
     */
    public String getRevision()
    {
        return "";
    }

    protected Version( String atrifactId, String version )
    {
        super( atrifactId );
        Package pkg = getClass().getPackage();
        this.title = defaultValue( pkg.getImplementationTitle(), atrifactId );
        this.vendor = defaultValue( pkg.getImplementationVendor(), "Neo Technology" );
        this.version = defaultValue( pkg.getImplementationVersion(), version );
    }

    private static String defaultValue( String preferred, String fallback )
    {
        return ( preferred == null || preferred.equals( "" ) ) ? fallback : preferred;
    }

    public static Version getKernel()
    {
        return KERNEL_VERSION;
    }

    public static String getKernelRevision()
    {
        return KERNEL_VERSION.getRevision();
    }
    
    /**
     * A very nice to have main-method for quickly checking the version of a neo4j kernel,
     * for example given a kernel jar file.
     */
    public static void main( String[] args )
    {
        ComponentVersion componentVersion = new ComponentVersion();
        System.out.println( "Release version: " + componentVersion.getReleaseVersion() );
        System.out.println( "Version: " + componentVersion.getVersion() );
        System.out.println( "Revision: " + componentVersion.getRevision() );
    }
}
