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
package org.neo4j.commandline.dbms;

import picocli.CommandLine.Parameters;

import java.io.Closeable;
import java.io.File;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static picocli.CommandLine.Command;

@Command(
        name = "store-info",
        header = "Print information about a Neo4j database store.",
        description = "Print information about a Neo4j database store, such as what version of Neo4j created it."
)
public class StoreInfoCommand extends AbstractCommand
{
    @Parameters( description = "Path to database store." )
    private File storePath;

    public StoreInfoCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute()
    {
        Validators.CONTAINS_EXISTING_DATABASE.validate( storePath );

        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( storePath );
        try ( Closeable ignored = LockChecker.checkDatabaseLock( databaseLayout );
              JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( ctx.fs(), jobScheduler ) )
        {
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
            StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( ctx.fs(), databaseLayout, Config.defaults(), pageCache,
                    NullLogService.getInstance() );
            String storeVersion = storeVersionCheck.storeVersion()
                    .orElseThrow( () -> new CommandFailedException( format( "Could not find version metadata in store '%s'", storePath ) ) );

            final String fmt = "%-30s%s";
            ctx.out().println( format( fmt, "Store format version:", storeVersion ) );

            StoreVersion versionInformation = storageEngineFactory.versionInformation( storeVersion );
            ctx.out().println( format( fmt, "Store format introduced in:", versionInformation.introductionNeo4jVersion() ) );

            versionInformation.successor()
                    .map( next -> format( fmt, "Store format superseded in:", next.introductionNeo4jVersion() ) )
                    .ifPresent( ctx.out()::println );
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + databaseLayout.getDatabaseName() + "' and try again.", e );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
