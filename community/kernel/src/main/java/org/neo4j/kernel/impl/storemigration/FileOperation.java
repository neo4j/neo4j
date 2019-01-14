/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
                File toDirectory, ExistingTargetStrategy existingTargetStrategy )
                throws IOException
        {
            File fromFile = fromFile( fs, fromDirectory, fileName, skipNonExistentFromFile );
            if ( fromFile != null )
            {
                File toFile = toFile( fs, toDirectory, fileName, existingTargetStrategy );
                if ( toFile != null )
                {
                    fs.copyFile( fromFile, toFile );
                }
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
                File toDirectory, ExistingTargetStrategy existingTargetStrategy )
                throws IOException
        {
            File fromFile = fromFile( fs, fromDirectory, fileName, skipNonExistentFromFile );
            if ( fromFile != null )
            {
                if ( toFile( fs, toDirectory, fileName, existingTargetStrategy ) != null )
                {
                    fs.moveToDirectory( fromFile, toDirectory );
                }
            }
        }
    },
    DELETE
    {
        @Override
        public void perform( FileSystemAbstraction fs, String fileName,
                File directory, boolean skipNonExistentFromFile,
                File unusedFile, ExistingTargetStrategy unused )
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
            File toDirectory, ExistingTargetStrategy existingTargetStrategy ) throws IOException;

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

    protected File toFile( FileSystemAbstraction fs, File directory, String name,
            ExistingTargetStrategy existingTargetStrategy )
    {
        File file = new File( directory, name );
        if ( fs.fileExists( file ) )
        {
            switch ( existingTargetStrategy )
            {
            case FAIL:
                // Let the copy operation fail. Is this a good idea? This is how we did before this switch case
            case OVERWRITE:
                fs.deleteFile( file );
                return file;
            case SKIP:
                return null;
            default:
                throw new IllegalStateException( existingTargetStrategy.name() );
            }
        }
        return file;
    }
}
