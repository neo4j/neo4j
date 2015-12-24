/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.layout.IndexFolderLayout;

import static java.util.stream.Collectors.toList;

public class PartitionedIndexStorage extends AbstractIndexStorage
{
    public PartitionedIndexStorage( DirectoryFactory directoryFactory, FileSystemAbstraction fileSystem,
            File schemaIndexRootFolder, long indexId )
    {
        super( directoryFactory, fileSystem, new IndexFolderLayout( schemaIndexRootFolder, indexId ) );
    }

    public Map<File, Directory> openIndexDirectories() throws IOException
    {
        Map<File, Directory> directories = new LinkedHashMap<>();
        try
        {
            File[] files = fileSystem.listFiles( getIndexFolder() );
            for ( File file : files )
            {
                if ( fileSystem.isDirectory( file ) )
                {
                    directories.put( file, directoryFactory.open( file ) );
                }
            }
        }
        catch ( IOException oe )
        {
            try
            {
                IOUtils.closeAll( directories.values() );
            }
            catch ( Exception ce )
            {
                oe.addSuppressed( ce );
            }
            throw oe;
        }
        return directories;
    }
}
