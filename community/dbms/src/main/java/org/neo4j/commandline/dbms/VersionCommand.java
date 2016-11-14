/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;

import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findSuccessor;

public class VersionCommand implements AdminCommand
{
    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryCanonicalPath( "store", "path-to-dir",
                    "Path to database store to check version of." ) );

    public static class Provider extends AdminCommand.Provider
    {

        public Provider()
        {
            super( "version" );
        }

        @Override
        public Arguments allArguments()
        {
            return arguments;
        }

        @Override
        public String summary()
        {
            return "Check the version of a Neo4j database store.";
        }

        @Override
        public String description()
        {
            return "Checks the version of a Neo4j database store. Note that this command expects a path to a store " +
                    "directory, for example --store=data/databases/graph.db.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new VersionCommand( outsideWorld::stdOutLine );
        }
    }

    private Consumer<String> out;

    public VersionCommand( Consumer<String> out )
    {
        this.out = out;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        final Path storeDir = arguments.parseMandatoryPath( "store", args );

        Validators.CONTAINS_EXISTING_DATABASE.validate( storeDir.toFile() );

        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction() ) )
        {
            final String storeVersion = new StoreVersionCheck( pageCache )
                    .getVersion( storeDir.resolve( MetaDataStore.DEFAULT_NAME ).toFile() )
                    .orElseThrow(
                            () -> new CommandFailed( String.format( "Could not find version metadata in store '%s'",
                                    storeDir ) ) );

            final String fmt = "%-25s%s";
            out.accept( String.format( fmt, "Store format version:", storeVersion ) );

            RecordFormats format = RecordFormatSelector.selectForVersion( storeVersion );
            out.accept( String.format( fmt, "Introduced in version:", format.introductionVersion() ) );

            findSuccessor( format )
                    .map( next -> String.format( fmt, "Superseded in version:", next.introductionVersion() ) )
                    .ifPresent( out );

            out.accept( String.format( fmt, "Current version:", Version.getNeo4jVersion() ) );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }
}
