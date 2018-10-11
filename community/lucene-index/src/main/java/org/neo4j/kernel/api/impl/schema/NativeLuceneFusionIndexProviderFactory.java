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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;

import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

public abstract class NativeLuceneFusionIndexProviderFactory<DEPENDENCIES> extends KernelExtensionFactory<DEPENDENCIES>
{
    public static final String KEY = LuceneIndexProviderFactory.KEY + "+" + NumberIndexProvider.KEY;

    NativeLuceneFusionIndexProviderFactory()
    {
        super( KEY );
    }

    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir, IndexProvider.Descriptor descriptor )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( descriptor );
        return directoriesBySubProvider( parentDirectoryStructure );
    }
}
