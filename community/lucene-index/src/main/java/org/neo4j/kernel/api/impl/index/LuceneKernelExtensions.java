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
package org.neo4j.kernel.api.impl.index;

import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.io.fs.FileSystemAbstraction;

public class LuceneKernelExtensions
{
    public static DirectoryFactory directoryFactory( boolean ephemeral, FileSystemAbstraction fileSystem )
    {
        if ( ephemeral )
        {
            return fileSystem.getOrCreateThirdPartyFileSystem( DirectoryFactory.class, IN_MEMORY_FACTORY );
        }
        return fileSystem.getOrCreateThirdPartyFileSystem( DirectoryFactory.class,
                Functions.<Class<DirectoryFactory>, DirectoryFactory>constant( DirectoryFactory.PERSISTENT ) );
    }

    public static final Function<Class<DirectoryFactory>, DirectoryFactory> IN_MEMORY_FACTORY =
            new Function<Class<DirectoryFactory>, DirectoryFactory>()
    {
        @Override
        public DirectoryFactory apply( Class<DirectoryFactory> directoryFactoryClass )
        {
            return new DirectoryFactory.InMemoryDirectoryFactory();
        }
    };
}
