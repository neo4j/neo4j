/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.monitoring.Monitors;

public class TextIndexProvider extends AbstractLuceneIndexProvider
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "text", "1.0" );

    public TextIndexProvider( FileSystemAbstraction fileSystem,
                              DirectoryFactory directoryFactory,
                              IndexDirectoryStructure.Factory directoryStructureFactory,
                              Monitors monitors, Config config,
                              DatabaseReadOnlyChecker readOnlyChecker )
    {
        super( IndexType.TEXT, DESCRIPTOR, fileSystem, directoryFactory, directoryStructureFactory, monitors, config, readOnlyChecker );
    }
}
