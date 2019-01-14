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
package org.neo4j.io.fs;

import java.io.RandomAccessFile;

/**
 * Modes describing how {@link StoreChannel} can be opened using {@link FileSystemAbstraction}.
 * <br/>
 * <br/>
 * Possible values:
 * <ul>
 * <li>
 * {@link #READ}:  Open for reading only.  Invoking any of the <b>write</b>
 * methods of the resulting object will cause an {@link java.io.IOException} to be thrown.
 * </li>
 * <li>
 * {@link #READ_WRITE}: Open for reading and writing.  If the file does not already
 * exist then an attempt will be made to create it.
 * </li>
 * <li>
 * {@link #SYNC}: Open for reading and writing, as with {@link #READ_WRITE}, and also
 * require that every update to the file's content or metadata be written synchronously to the underlying storage
 * device.
 * </li>
 * <li>
 * {@link #DSYNC}:  Open for reading and writing, as with {@link #READ_WRITE}, and also
 * require that every update to the file's content be written
 * synchronously to the underlying storage device.
 * </li>
 * </ul>
 *
 * @see RandomAccessFile
 */
public enum OpenMode
{
    READ( "r" ),
    READ_WRITE( "rw" ),
    SYNC( "rws" ),
    DSYNC( "rwd" );

    private final String mode;

    OpenMode( String mode )
    {
        this.mode = mode;
    }

    public String mode()
    {
        return mode;
    }
}
