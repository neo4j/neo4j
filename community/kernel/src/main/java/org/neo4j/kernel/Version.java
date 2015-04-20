/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class Version extends Service
{
    public static Version getKernel()
    {
        return KERNEL_VERSION;
    }

    public static String getKernelRevision()
    {
        return getKernel().getRevision();
    }

    private final String artifactId;
    private final String title;
    private final String vendor;
    private final String version;

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if ( title != null )
        {
            result.append( title );
            if ( artifactId == null || !artifactId.equals( title ) )
            {
                result.append( " (" ).append( artifactId ).append( ')' );
            }
        }
        else if ( artifactId != null )
        {
            result.append( artifactId );
        }
        else
        {
            result.append( "Unknown Component" );
        }
        result.append( ", " );
        if ( title == null )
        {
            result.append( "unpackaged " );
        }
        result.append( "version: " ).append( getVersion() );
        return result.toString();
    }

    /**
     * Gets the version of the running neo4j kernel.
     *
     * @return the version of the neo4j kernel
     */
    public final String getVersion()
    {
        if ( version == null || version.equals( "" ) )
        {
            return "revision: " + getRevision();
        }
        else if ( version.endsWith( "-SNAPSHOT" ) )
        {
            return version + " (revision: " + getRevision() + ")";
        }
        else
        {
            return version;
        }
    }

    public String getReleaseVersion()
    {
        return version;
    }

    /**
     * Returns the build revision of the running neo4j kernel.
     *
     * @return build revision
     */
    public final String getRevision()
    {
        StringBuilder result = new StringBuilder( getReleaseVersion() );
        result.append( ':' ).append( getBranchName() ).append( ':' );
        String build = getBuildNumber();
        if ( !(build.startsWith( "${" ) || build.startsWith( "{" )) )
        {
            result.append( build ).append( '/' );
        }
        result.append( getCommitId() );
        if ( getCommitDescription().endsWith( "-dirty" ) )
        {
            result.append( "-dirty" );
        }
        return result.toString();
    }

    protected String getCommitDescription()
    {
        return "{CommitDescription}";
    }

    protected String getBuildNumber()
    {
        return "{BuildNumber}";
    }

    protected String getCommitId()
    {
        return "{CommitId}";
    }

    protected String getBranchName()
    {
        return "{BranchName}";
    }

    protected Version( String artifactId, String version )
    {
        super( artifactId );
        this.artifactId = artifactId;
        Package pkg = getClass().getPackage();
        this.title = defaultValue( pkg.getImplementationTitle(), artifactId );
        this.vendor = defaultValue( pkg.getImplementationVendor(), "Neo Technology" );
        this.version = defaultValue( pkg.getImplementationVersion(), version );
    }

    private static String defaultValue( String preferred, String fallback )
    {
        return (preferred == null || preferred.equals( "" )) ? fallback : preferred;
    }

    /**
     * A very nice to have main-method for quickly checking the version of a neo4j kernel,
     * for example given a kernel jar file.
     */
    public static void main( String[] args )
    {
        Version kernelVersion = getKernel();
        System.out.println( kernelVersion );
        System.out.println( "Title: " + kernelVersion.title );
        System.out.println( "Vendor: " + kernelVersion.vendor );
        System.out.println( "ArtifactId: " + kernelVersion.artifactId );
        System.out.println( "Version: " + kernelVersion.getVersion() );
        System.out.println( "ReleaseVersion: " + kernelVersion.getReleaseVersion() );
        System.out.println( "Revision: " + kernelVersion.getRevision() );
        System.out.println( "CommitDescription: " + kernelVersion.getCommitDescription() );
        System.out.println( "BuildNumber: " + kernelVersion.getBuildNumber() );
        System.out.println( "BranchName: " + kernelVersion.getBranchName() );
        System.out.println( "CommitId: " + kernelVersion.getCommitId() );
    }

    static final String KERNEL_ARTIFACT_ID = "neo4j-kernel";
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
            kernelVersion = null;
        }
        if ( kernelVersion == null )
        {
            try
            {
                kernelVersion = (Version) Class.forName( "org.neo4j.kernel.impl.ComponentVersion" ).newInstance();
            }
            catch ( Exception e )
            {
                kernelVersion = null;
            }
        }
        if ( kernelVersion == null )
        {
            kernelVersion = new Version( KERNEL_ARTIFACT_ID, "" );
        }
        KERNEL_VERSION = kernelVersion;
    }
}
