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
package org.neo4j.index;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.test.rule.DbmsRule;

abstract class NativeIndexRestartAction implements DbmsRule.RestartAction
{
    private static final IndexProviderDescriptor DEFAULT_PROVIDER_DESCRIPTOR = GenericNativeIndexProvider.DESCRIPTOR;
    final IndexProviderDescriptor providerDescriptor;

    NativeIndexRestartAction()
    {
        this( DEFAULT_PROVIDER_DESCRIPTOR );
    }

    NativeIndexRestartAction( IndexProviderDescriptor providerDescriptor )
    {
        this.providerDescriptor = providerDescriptor;
    }

    @Override
    public void run( FileSystemAbstraction fs, DatabaseLayout databaseLayout ) throws IOException
    {
        IndexDirectoryStructure indexDirectoryStructure = nativeIndexDirectoryStructure( databaseLayout, providerDescriptor );
        runOnDirectoryStructure( fs, indexDirectoryStructure );
    }

    protected abstract void runOnDirectoryStructure( FileSystemAbstraction fs, IndexDirectoryStructure indexDirectoryStructure ) throws IOException;

    static IndexDirectoryStructure nativeIndexDirectoryStructure( DatabaseLayout databaseLayout )
    {
        return IndexDirectoryStructure.directoriesByProvider( databaseLayout.databaseDirectory() ).forProvider( DEFAULT_PROVIDER_DESCRIPTOR );
    }

    static IndexDirectoryStructure nativeIndexDirectoryStructure( DatabaseLayout databaseLayout, IndexProviderDescriptor providerDescriptor )
    {
        return IndexDirectoryStructure.directoriesByProvider( databaseLayout.databaseDirectory() ).forProvider( providerDescriptor );
    }
}
