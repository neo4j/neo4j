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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.neo4j.common.Validator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

public final class Validators
{
    public static final Validator<String> REGEX_FILE_EXISTS = fileWithRegexInName ->
    {
        File file = new File( fileWithRegexInName ); // Path can't handle regex in the name
        File parent = file.getParentFile();
        if ( parent == null )
        {
            throw new IllegalArgumentException( "Directory of " + fileWithRegexInName + " doesn't exist" );
        }

        if ( matchingFiles( parent.toPath(), file.getName() ).isEmpty() )
        {
            throw new IllegalArgumentException( "File '" + file + "' doesn't exist" );
        }
    };

    private Validators()
    {
    }

    static List<Path> matchingFiles( Path directory, String fileWithRegexInName )
    {
        if ( Files.notExists( directory ) || !Files.isDirectory( directory ) )
        {
            throw new IllegalArgumentException( directory + " is not a directory" );
        }
        final Pattern pattern = Pattern.compile( fileWithRegexInName );
        List<Path> files = new ArrayList<>();
        try ( Stream<Path> list = Files.list( directory ) )
        {
            list.forEach( file ->
            {
                if ( pattern.matcher( file.getFileName().toString() ).matches() )
                {
                    files.add( file );
                }
            } );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return files;
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
