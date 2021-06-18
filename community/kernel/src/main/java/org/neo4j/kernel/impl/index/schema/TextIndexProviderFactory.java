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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.schema.IndexProviderFactoryUtil;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.RecoveryExtension;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@ServiceProvider
@RecoveryExtension
public class TextIndexProviderFactory extends ExtensionFactory<TextIndexProviderFactory.Dependencies>
{
    private static final String KEY = "text";
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction fileSystem();

        Monitors monitors();

        DatabaseReadOnlyChecker readOnlyChecker();
    }

    public TextIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, KEY );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, TextIndexProviderFactory.Dependencies dependencies )
    {
        FileSystemAbstraction fs = dependencies.fileSystem();
        IndexDirectoryStructure.Factory directoryStructure = directoriesByProvider( context.directory() );
        return IndexProviderFactoryUtil
                .textProvider( fs, directoryStructure, dependencies.monitors(), dependencies.getConfig(), dependencies.readOnlyChecker() );
    }
}
