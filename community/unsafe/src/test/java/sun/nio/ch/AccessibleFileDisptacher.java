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
package sun.nio.ch;

import java.io.FileDescriptor;
import java.io.IOException;

public abstract class AccessibleFileDisptacher extends FileDispatcher
{
    // FileDispatcher:
    public abstract int force(FileDescriptor fd, boolean metaData) throws IOException;
    public abstract int force(FileDescriptor fd, boolean metaData, boolean writable) throws IOException;
    public abstract int truncate(FileDescriptor fd, long size) throws IOException;
    public abstract long size(FileDescriptor fd) throws IOException;
    public abstract int lock(FileDescriptor fd, boolean blocking, long pos, long size, boolean shared) throws IOException;
    public abstract void release(FileDescriptor fd, long pos, long size) throws IOException;
    public abstract FileDescriptor duplicateForMapping(FileDescriptor fd) throws IOException;

    // NativeDispatcher:
    public abstract int read(FileDescriptor fd, long address, int len) throws IOException;
    public boolean needsPositionLock() { return super.needsPositionLock(); }
    public int pread(FileDescriptor fd, long address, int len, long position) throws IOException { return super.pread( fd, address, len, position ); }
    public abstract long readv(FileDescriptor fd, long address, int len) throws IOException;
    public abstract int write(FileDescriptor fd, long address, int len) throws IOException;
    public int pwrite(FileDescriptor fd, long address, int len, long position) throws IOException { return super.pwrite( fd, address, len, position ); }
    public abstract long writev(FileDescriptor fd, long address, int len) throws IOException;
    public abstract void close(FileDescriptor fd) throws IOException;
    public void preClose(FileDescriptor fd) throws IOException { }
}
