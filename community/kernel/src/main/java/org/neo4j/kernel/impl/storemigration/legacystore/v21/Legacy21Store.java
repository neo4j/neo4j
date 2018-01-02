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
package org.neo4j.kernel.impl.storemigration.legacystore.v21;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;

/**
 * Reader for a database in an older store format version.
 * <p>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the reader code is specific for the current upgrade and changes with each store format version.
 * <p>
 * {@link #LEGACY_VERSION} marks which version it's able to read.
 */
public class Legacy21Store implements LegacyStore
{
    public static final String LEGACY_VERSION = "v0.A.3";
    private final File storageFileName;

    public Legacy21Store( File storageFileName ) throws IOException
    {
        this.storageFileName = storageFileName;
        assertLegacyAndCurrentVersionHaveSameLength( LEGACY_VERSION, CommonAbstractStore.ALL_STORES_VERSION );
    }

    /**
     * Store files that don't need migration are just copied and have their trailing versions replaced
     * by the current version. For this to work the legacy version and the current version must have the
     * same encoded length.
     */
    static void assertLegacyAndCurrentVersionHaveSameLength( String legacyVersion, String currentVersion )
    {
        if ( UTF8.encode( legacyVersion ).length != UTF8.encode( currentVersion ).length )
        {
            throw new IllegalStateException( "Encoded version string length must remain the same between versions" );
        }
    }

    @Override
    public File getStorageFileName()
    {
        return storageFileName;
    }

    @Override
    public void close() throws IOException
    {
        // nothing to close
    }

    @Override
    public org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader getNodeStoreReader()
    {
        // not needed
        throw new UnsupportedOperationException();
    }

    @Override
    public org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader getRelStoreReader()
    {
        // not needed
        throw new UnsupportedOperationException();
    }
}
