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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Different operations on a file, for example copy or move, given a {@link FileSystemAbstraction} and
 * source/destination.
 */
public enum FileOperation
{
    COPY
    {
        /**
         * Copies a file from one directory to another.
         *
         * @param fs the {@link FileSystemAbstraction} the file exist on
         * @param fileName base filename of the file to move, not the complete path
         * @param fromDirectory directory currently containing filename
         * @param toDirectory directory to host filename
         * @throws IOException if the file couldn't be copied
         */
        @Override
        public void perform( FileSystemAbstraction fs, String fileName,
                File fromDirectory, boolean skipNonExistentFromFile,
                File toDirectory, boolean overwrite )
                throws IOException
        {
            File fromFile = fromFile( fs, fromDirectory, fileName, skipNonExistentFromFile );
            if ( fromFile != null )
            {
                fs.copyFile( fromFile, toFile( fs, toDirectory, fileName, overwrite ) );
            }
        }
    },
    MOVE
    {
        /**
         * Moves a file from one directory to another, by a rename op.
         *
         * @param fs the {@link FileSystemAbstraction} the file exist on
         * @param fileName base filename of the file to move, not the complete path
         * @param fromDirectory directory currently containing filename
         * @param toDirectory directory to host filename - must be in the same disk partition as filename
         * @throws IOException if the file couldn't be moved
         */
        @Override
        public void perform( FileSystemAbstraction fs, String fileName,
                File fromDirectory, boolean skipNonExistentFromFile,
                File toDirectory, boolean overwrite )
                throws IOException
        {
            File fromFile = fromFile( fs, fromDirectory, fileName, skipNonExistentFromFile );
            if ( fromFile != null )
            {
                toFile( fs, toDirectory, fileName, overwrite );
                fs.moveToDirectory( fromFile, toDirectory );
            }
        }
    },
    DELETE
    {
        @Override
        public void perform( FileSystemAbstraction fs, String fileName,
                File directory, boolean skipNonExistentFromFile,
                File unusedFile, boolean unusedBoolean )
                throws IOException
        {
            File file = fromFile( fs, directory, fileName, skipNonExistentFromFile );
            if ( file != null )
            {
                fs.deleteFile( file );
            }
        }
    };

    public abstract void perform( FileSystemAbstraction fs, String fileName,
            File fromDirectory, boolean skipNonExistentFromFile,
            File toDirectory, boolean overwrite ) throws IOException;

    protected File fromFile( FileSystemAbstraction fs, File directory, String name, boolean skipNonExistent )
    {
        File fromFile = new File( directory, name );
        if ( skipNonExistent && !fs.fileExists( fromFile ) )
        {
            return null;
        }
        // Return the file even if it doesn't exist here (and we don't allow skipping) so that the actual
        // file operation will fail later
        return fromFile;
    }

    protected File toFile( FileSystemAbstraction fs, File directory, String name, boolean overwrite )
    {
        File file = new File( directory, name );
        if ( overwrite )
        {
            fs.deleteFile( file );
        }
        return file;
    }
}
