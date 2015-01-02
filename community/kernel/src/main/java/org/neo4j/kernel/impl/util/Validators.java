/**
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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;

public class Validators
{
    public static final Validator<File[]> FILES_EXISTS = new Validator<File[]>()
    {
        @Override
        public void validate( File[] files )
        {
            for ( File file : files )
            {
                if ( !file.exists() )
                {
                    throw new IllegalArgumentException( file + " doesn't exist" );
                }
            }
        }
    };

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
            if ( NeoStoreUtil.neoStoreExists( new DefaultFileSystemAbstraction(), value ) )
            {
                throw new IllegalArgumentException( "Directory '" + value + "' already contains a database" );
            }
        }
    };

    public static <T> Validator<T[]> atLeast( final int length )
    {
        return new Validator<T[]>()
        {
            @Override
            public void validate( T[] value )
            {
                if ( value.length < length )
                {
                    throw new IllegalArgumentException( "Expected " + Arrays.toString( value ) +
                            " to have at least " + length + " items, but had only " + value.length );
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
