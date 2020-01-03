/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

public abstract class NativeLuceneFusionIndexProviderFactory<DEPENDENCIES extends AbstractIndexProviderFactory.Dependencies>
        extends AbstractIndexProviderFactory<DEPENDENCIES>
{
    NativeLuceneFusionIndexProviderFactory( String key )
    {
        super( key );
    }

    @Override
    protected Class loggingClass()
    {
        return FusionIndexProvider.class;
    }

    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File databaseDirectory, IndexProviderDescriptor descriptor )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( databaseDirectory ).forProvider( descriptor );
        return directoriesBySubProvider( parentDirectoryStructure );
    }
}
