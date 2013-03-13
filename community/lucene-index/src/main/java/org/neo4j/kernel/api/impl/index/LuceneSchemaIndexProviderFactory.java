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
package org.neo4j.kernel.api.impl.index;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

@Service.Implementation( KernelExtensionFactory.class )
public class LuceneSchemaIndexProviderFactory extends KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>
{
    static final String KEY = "lucene";

    public interface Dependencies
    {
        Config getConfig();
        
        FileSystemAbstraction getFileSystemAbstraction();
    }
    
    public LuceneSchemaIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public LuceneSchemaIndexProvider newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return new LuceneSchemaIndexProvider( directoryFactory( dependencies ),
                dependencies.getFileSystemAbstraction(), dependencies.getConfig() );
    }

    private DirectoryFactory directoryFactory( Dependencies dependencies )
    {
        if ( dependencies.getFileSystemAbstraction() instanceof EphemeralFileSystemAbstraction )
        {
            return DirectoryFactory.IN_MEMORY;
        }
        else if ( dependencies.getFileSystemAbstraction() instanceof DefaultFileSystemAbstraction )
        {
            return DirectoryFactory.PERSISTENT;
        }
        throw new IllegalArgumentException( "Unsupported file system " + dependencies.getFileSystemAbstraction() );
    }
}
