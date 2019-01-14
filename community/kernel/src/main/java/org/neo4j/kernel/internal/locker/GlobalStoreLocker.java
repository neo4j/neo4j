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
package org.neo4j.kernel.internal.locker;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.StoreLockException;

/**
 * Store locker that guarantee that only single channel ever will be opened and then closed for store locker
 * file to prevent cases where lock will be released when any of the opened channels for file will be released as
 * described in {@link FileLock} javadoc:
 * <p>
 * <b>
 * On some systems, closing a channel releases all locks held by the Java virtual machine on the underlying
 * file regardless of whether the locks were acquired via that channel or via another channel open on the same file.
 * It is strongly recommended that, within a program, a unique channel be used to acquire all locks on any given file.
 * </b>
 * </p>
 *
 * The guarantee is achieved by tracking all locked files over all instances of {@link GlobalStoreLocker}.
 *
 * Class guarantee visibility of locked files over multiple thread but do not guarantee atomicity of operations.
 */
public class GlobalStoreLocker extends StoreLocker
{
    private static final Set<File> lockedFiles = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public GlobalStoreLocker( FileSystemAbstraction fileSystemAbstraction, File storeDirectory )
    {
        super( fileSystemAbstraction, storeDirectory );
    }

    @Override
    public void checkLock() throws StoreLockException
    {
        super.checkLock();
        lockedFiles.add( storeLockFile );
    }

    @Override
    protected boolean haveLockAlready()
    {
        if ( lockedFiles.contains( storeLockFile ) )
        {
            if ( storeLockFileLock != null )
            {
                return true;
            }
            throw unableToObtainLockException();
        }
        return false;
    }

    @Override
    protected void releaseLock() throws IOException
    {
        lockedFiles.remove( storeLockFile );
        super.releaseLock();
    }
}
