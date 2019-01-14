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

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.util.Validators;

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
        final Path storeDir = arguments.parse( args ).getMandatoryPath( "store" );

        Validators.CONTAINS_EXISTING_DATABASE.validate( storeDir.toFile() );

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem ) )
        {
            final String storeVersion = new StoreVersionCheck( pageCache )
                    .getVersion( storeDir.resolve( MetaDataStore.DEFAULT_NAME ).toFile() )
                    .orElseThrow(
                            () -> new CommandFailed( String.format( "Could not find version metadata in store '%s'",
                                    storeDir ) ) );

            final String fmt = "%-30s%s";
            out.accept( String.format( fmt, "Store format version:", storeVersion ) );

            RecordFormats format = RecordFormatSelector.selectForVersion( storeVersion );
            out.accept( String.format( fmt, "Store format introduced in:", format.introductionVersion() ) );

            findSuccessor( format )
                    .map( next -> String.format( fmt, "Store format superseded in:", next.introductionVersion() ) )
                    .ifPresent( out );

            //out.accept( String.format( fmt, "Current version:", Version.getNeo4jVersion() ) );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
