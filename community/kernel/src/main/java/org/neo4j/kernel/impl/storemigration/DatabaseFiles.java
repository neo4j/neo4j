/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

public class DatabaseFiles
{
    public void moveToBackupDirectory( File workingDirectory, File backupDirectory )
    {
        if (backupDirectory.exists())
        {
            throw new StoreUpgrader.UnableToUpgradeException( String.format( "Cannot proceed with upgrade " +
                    "because there is an existing upgrade backup in the way at %s. If you do not need this " +
                    "backup please delete it or move it out of the way before re-attempting upgrade.",
                    backupDirectory.getAbsolutePath() ) );
        }
        backupDirectory.mkdir();
        move( workingDirectory, backupDirectory );
    }

    public void moveToWorkingDirectory( File upgradeDirectory, File workingDirectory )
    {
        move( upgradeDirectory, workingDirectory );
    }

    private void move( File fromDirectory, File toDirectory )
    {
        try
        {
            StoreFiles.move( fromDirectory, toDirectory );
            LogFiles.move( fromDirectory, toDirectory );
        }
        catch ( IOException e )
        {
            throw new StoreUpgrader.UnableToUpgradeException( e );
        }
    }
}
