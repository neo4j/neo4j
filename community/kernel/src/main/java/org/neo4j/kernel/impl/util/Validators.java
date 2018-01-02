/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreFileType;

public class Validators
{
    public static final Validator<File> REGEX_FILE_EXISTS = new Validator<File>()
    {
        @Override
        public void validate( File file )
        {
            if ( matchingFiles( file ).isEmpty() )
            {
                throw new IllegalArgumentException( "File '" + file + "' doesn't exist" );
            }
        }
    };

    static List<File> matchingFiles( File fileWithRegexInName )
    {
        File parent = fileWithRegexInName.getAbsoluteFile().getParentFile();
        if ( parent == null || !parent.exists() )
        {
            throw new IllegalArgumentException( "Directory of " + fileWithRegexInName + " doesn't exist" );
        }
        final Pattern pattern = Pattern.compile( fileWithRegexInName.getName() );
        List<File> files = new ArrayList<>();
        for ( File file : parent.listFiles() )
        {
            if ( pattern.matcher( file.getName() ).matches() )
            {
                files.add( file );
            }
        }
        return files;
    }

    public static final Validator<File> DIRECTORY_IS_WRITABLE = new Validator<File>()
    {
        @Override
        public void validate( File value )
        {
            if ( value.mkdirs() )
            {   // It's OK, we created the directory right now, which means we have write access to it
                return;
            }

            File test = new File( value, "_______test___" );
            try
            {
                test.createNewFile();
            }
            catch ( IOException e )
            {
                throw new IllegalArgumentException( "Directoy '" + value + "' not writable: " + e.getMessage() );
            }
            finally
            {
                test.delete();
            }
        }
    };

    public static final Validator<File> CONTAINS_NO_EXISTING_DATABASE = new Validator<File>()
    {
        @Override
        public void validate( File value )
        {
            if ( new DefaultFileSystemAbstraction()
                    .fileExists( new File( value, StoreFileType.STORE.augment( MetaDataStore.DEFAULT_NAME ) ) ) )
            {
                throw new IllegalArgumentException( "Directory '" + value + "' already contains a database" );
            }
        }
    };

    public static <T> Validator<T[]> atLeast( final String key, final int length )
    {
        return new Validator<T[]>()
        {
            @Override
            public void validate( T[] value )
            {
                if ( value.length < length )
                {
                    throw new IllegalArgumentException( "Expected '" + key + "' to have at least " +
                            length + " valid item" + (length == 1 ? "" : "s") + ", but had " + value.length +
                            " " + Arrays.toString( value ) );
                }
            }
        };
    }

    public static final <T> Validator<T> emptyValidator()
    {
        return new Validator<T>()
        {
            @Override
            public void validate( T value )
            {   // Do nothing
            }
        };
    }
}
