/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.common.Validator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.util.Preconditions;

public final class Validators
{
    public static final Validator<String> REGEX_FILE_EXISTS = fileWithRegexInName ->
    {
        if ( matchingFiles( fileWithRegexInName ).isEmpty() )
        {
            throw new IllegalArgumentException( "File '" + fileWithRegexInName + "' doesn't exist" );
        }
    };

    private Validators()
    {
    }

    static List<Path> matchingFiles( String fileWithRegexInName )
    {
        // Special handling of regex patterns for Windows since Windows paths naturally contains \ characters and also regex can contain those
        // so in order to support this on Windows then \\ will be required in regex patterns and will not be treated as directory delimiter.
        // Get those double backslashes out of the way so that we can trust the File operations to return correct parent etc.
        String parentSafeFileName = fileWithRegexInName.replace( "\\\\", "__" );
        File absoluteParentSafeFile = new File( parentSafeFileName ).getAbsoluteFile();
        File parent = absoluteParentSafeFile.getParentFile();
        Preconditions.checkState( parent != null && parent.exists(), "Directory %s of %s doesn't exist", parent, fileWithRegexInName );

        // Then since we can't trust the file operations to do the right thing on Windows if there are regex backslashes we instead
        // get the pattern by cutting off the parent directory from the name manually.
        int fileNameLength = absoluteParentSafeFile.getAbsolutePath().length() - parent.getAbsolutePath().length() - 1;
        String patternString = fileWithRegexInName.substring( fileWithRegexInName.length() - fileNameLength ).replace( "\\\\", "\\" );
        final Pattern pattern = Pattern.compile( patternString );
        List<Path> paths = new ArrayList<>();
        for ( File file : parent.listFiles() )
        {
            if ( pattern.matcher( file.getName() ).matches() )
            {
                paths.add( file.toPath() );
            }
        }
        return paths;
    }

    public static final Validator<Path> CONTAINS_EXISTING_DATABASE = dbDir ->
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            if ( !isExistingDatabase( fileSystem, DatabaseLayout.ofFlat( dbDir ) ) )
            {
                throw new IllegalArgumentException( "Directory '" + dbDir + "' does not contain a database" );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    };

    public static boolean isExistingDatabase( FileSystemAbstraction fileSystem, DatabaseLayout layout )
    {
        return fileSystem.fileExists( layout.metadataStore() );
    }

    public static <T> Validator<T> emptyValidator()
    {
        return value -> {};
    }
}
