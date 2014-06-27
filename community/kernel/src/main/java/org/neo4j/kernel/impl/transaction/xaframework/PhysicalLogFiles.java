/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.lang.Math.max;

/**
 * Used to figure out what logical log file to open when the database
 * starts up.
 */
public class PhysicalLogFiles
{
    private final File logBaseName;
    private final FileSystemAbstraction fileSystem;

    public PhysicalLogFiles( File directory, String name, FileSystemAbstraction fileSystem )
    {
        this.logBaseName = new File( directory, name );
        this.fileSystem = fileSystem;
    }

    public PhysicalLogFiles( File directory, FileSystemAbstraction fileSystem )
    {
        this( directory, PhysicalLogFile.DEFAULT_NAME, fileSystem );
    }

    private Pattern getVersionFileNamePattern()
    {
        return Pattern.compile( logBaseName.getName() + "\\.v\\d+" );
    }

    public File getVersionFileName( long version )
    {
        return new File( logBaseName.getPath() + ".v" + version );
    }

    public long getLogVersion( File historyLogFile )
    { // Get version based on the name
        String name = historyLogFile.getName();
        String toFind = ".v";
        int index = name.lastIndexOf( toFind );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + historyLogFile + "'" );
        }
        return Integer.parseInt( name.substring( index + toFind.length() ) );
    }

    public long getHighestLogVersion()
    {
        Pattern logFilePattern = getVersionFileNamePattern();
        long highest = -1;
        for ( File file : fileSystem.listFiles( logBaseName.getParentFile() ) )
        {
            if ( logFilePattern.matcher( file.getName() ).matches() )
            {
                highest = max( highest, getLogVersion( file ) );
            }
        }
        return highest;
    }

    public boolean versionExists( long version )
    {
        return fileSystem.fileExists( getVersionFileName( version ) );
    }

    public long[] extractHeader( long version ) throws IOException
    {
        return VersionAwareLogEntryReader.readLogHeader( fileSystem, getVersionFileName( version ) );
    }
}
