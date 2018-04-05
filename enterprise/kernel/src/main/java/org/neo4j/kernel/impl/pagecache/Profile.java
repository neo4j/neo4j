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
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.kernel.impl.pagecache.PageCacheWarmer.SUFFIX_CACHEPROF;

final class Profile implements Comparable<Profile>
{
    private static final String PROFILE_DIR = "profiles";
    private final File profileFile;
    private final File pagedFile;
    private final long profileSequenceId;

    private Profile( File profileFile, File pagedFile, long profileSequenceId )
    {
        Objects.requireNonNull( profileFile );
        Objects.requireNonNull( pagedFile );
        this.profileFile = profileFile;
        this.pagedFile = pagedFile;
        this.profileSequenceId = profileSequenceId;
    }

    @Override
    public int compareTo( Profile that )
    {
        int compare = pagedFile.compareTo( that.pagedFile );
        return compare == 0 ? Long.compare( profileSequenceId, that.profileSequenceId ) : compare;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof Profile )
        {
            Profile profile = (Profile) o;
            return profileFile.equals( profile.profileFile );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return profileFile.hashCode();
    }

    @Override
    public String toString()
    {
        return "Profile(" + profileSequenceId + " for " + pagedFile + ")";
    }

    File file()
    {
        return profileFile;
    }

    void delete( FileSystemAbstraction fs )
    {
        fs.deleteFile( profileFile );
    }

    InputStream read( FileSystemAbstraction fs ) throws IOException
    {
        InputStream source = fs.openAsInputStream( profileFile );
        try
        {
            return new GZIPInputStream( source );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( source );
            throw new IOException( "Exception when building decompressor.", e );
        }
    }

    OutputStream write( FileSystemAbstraction fs ) throws IOException
    {
        fs.mkdirs( profileFile.getParentFile() ); // Create PROFILE_FOLDER if it does not exist.
        OutputStream sink = fs.openAsOutputStream( profileFile, false );
        try
        {
            return new GZIPOutputStream( sink );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( sink );
            throw new IOException( "Exception when building compressor.", e );
        }
    }

    Profile next()
    {
        long next = profileSequenceId + 1L;
        return new Profile( profileName( pagedFile, next ), pagedFile, next );
    }

    static Profile first( File file )
    {
        return new Profile( profileName( file, 0 ), file, 0 );
    }

    private static File profileName( File file, long count )
    {
        String name = file.getName();
        File dir = new File( file.getParentFile(), PROFILE_DIR );
        return new File( dir, name + "." + Long.toString( count ) + SUFFIX_CACHEPROF );
    }

    static Predicate<Profile> relevantTo( PagedFile pagedFile )
    {
        return p -> p.pagedFile.equals( pagedFile.file() );
    }

    static Stream<Profile> findProfilesInDirectory( FileSystemAbstraction fs, File dir )
    {
        File[] files = fs.listFiles( new File( dir, PROFILE_DIR ) );
        if ( files == null )
        {
            return Stream.empty();
        }
        return Stream.of( files ).flatMap( Profile::parseProfileName );
    }

    private static Stream<Profile> parseProfileName( File profile )
    {
        File profileFolder = profile.getParentFile();
        File dir = profileFolder.getParentFile();
        String name = profile.getName();
        if ( !name.endsWith( SUFFIX_CACHEPROF ) )
        {
            return Stream.empty();
        }
        int lastDot = name.lastIndexOf( '.' );
        int secondLastDot = name.lastIndexOf( '.', lastDot - 1 );
        String countStr = name.substring( secondLastDot + 1, lastDot );
        try
        {
            long sequenceId = Long.parseLong( countStr, 10 );
            String mappedFileName = name.substring( 0, secondLastDot );
            return Stream.of( new Profile( profile, new File( dir, mappedFileName ), sequenceId ) );
        }
        catch ( NumberFormatException e )
        {
            return Stream.empty();
        }
    }
}
