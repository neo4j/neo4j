/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.util.StoreUtil;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BranchedDataMigrator extends LifecycleAdapter
{
    private final File storeDir;

    public BranchedDataMigrator( File storeDir )
    {
        this.storeDir = storeDir;
    }

    @Override
    public void start() throws Throwable
    {
        migrateBranchedDataDirectoriesToRootDirectory();
    }

    private void migrateBranchedDataDirectoriesToRootDirectory()
    {
        File branchedDir = StoreUtil.getBranchedDataRootDirectory( storeDir );
        branchedDir.mkdirs();
        for ( File oldBranchedDir : storeDir.listFiles() )
        {
            if ( !oldBranchedDir.isDirectory() || !oldBranchedDir.getName().startsWith( "branched-" ) )
            {
                continue;
            }

            long timestamp = 0;
            try
            {
                timestamp = Long.parseLong( oldBranchedDir.getName().substring( oldBranchedDir.getName().indexOf( '-'
                ) + 1 ) );
            }
            catch ( NumberFormatException e )
            {   // OK, it wasn't a branched directory after all.
                continue;
            }

            File targetDir = StoreUtil.getBranchedDataDirectory( storeDir, timestamp );
            try
            {
                FileUtils.moveFile( oldBranchedDir, targetDir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Couldn't move branched directories to " + branchedDir, e );
            }
        }
    }
}
