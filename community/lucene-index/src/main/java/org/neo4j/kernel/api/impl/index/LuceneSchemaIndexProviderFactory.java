/**
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
package org.neo4j.kernel.api.impl.index;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

@Service.Implementation(KernelExtensionFactory.class)
public class LuceneSchemaIndexProviderFactory extends
        KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>
{
    static final String KEY = "lucene";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction getFileSystem();
    }

    public LuceneSchemaIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public LuceneSchemaIndexProvider newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        Config config = dependencies.getConfig();
        FileSystemAbstraction fileSystem = dependencies.getFileSystem();
        DirectoryFactory directoryFactory = directoryFactory( config, fileSystem );
        return new LuceneSchemaIndexProvider( directoryFactory, config );
    }
}
