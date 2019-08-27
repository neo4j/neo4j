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

import java.io.Closeable;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findSuccessor;

public class StoreInfoCommand implements AdminCommand
{
    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryCanonicalPath( "store", "path-to-dir",
                    "Path to database store." ) );

    private Consumer<String> out;

    public StoreInfoCommand( Consumer<String> out )
    {
        this.out = out;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        final Path databaseDirectory = arguments.parse( args ).getMandatoryPath( "store" );

        Validators.CONTAINS_EXISTING_DATABASE.validate( databaseDirectory.toFile() );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDirectory.toFile() );
        try ( Closeable ignored = StoreLockChecker.check( databaseLayout.getStoreLayout() );
                DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                JobScheduler jobScheduler = createInitialisedScheduler();
                PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem, jobScheduler ) )
        {
            final String storeVersion = new StoreVersionCheck( pageCache )
                    .getVersion( databaseLayout.metadataStore() )
                    .orElseThrow( () -> new CommandFailed( String.format( "Could not find version metadata in store '%s'", databaseDirectory ) ) );

            final String fmt = "%-30s%s";
            out.accept( String.format( fmt, "Store format version:", storeVersion ) );

            RecordFormats format = RecordFormatSelector.selectForVersion( storeVersion );
            out.accept( String.format( fmt, "Store format introduced in:", format.introductionVersion() ) );

            findSuccessor( format )
                    .map( next -> String.format( fmt, "Store format superseded in:", next.introductionVersion() ) )
                    .ifPresent( out );
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "the database is in use -- stop Neo4j and try again", e );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
