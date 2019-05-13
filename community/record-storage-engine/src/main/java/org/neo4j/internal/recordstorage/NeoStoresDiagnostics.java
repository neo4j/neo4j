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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.Logger;
import org.neo4j.util.FeatureToggles;

public abstract class NeoStoresDiagnostics extends NamedDiagnosticsProvider
{
    private static final boolean THROW_NEOSTORE_DIAGNOSTICS_ERRORS =
            FeatureToggles.flag( NeoStoresDiagnostics.class, "THROW_NEOSTORE_DIAGNOSTICS_ERRORS", false );

    public static class NeoStoreVersions extends NeoStoresDiagnostics
    {
        NeoStoreVersions( NeoStores nodeStores )
        {
            super( nodeStores, "Store versions" );
        }

        @Override
        protected void dump( NeoStores neoStores, Logger logger )
        {
            neoStores.logVersions( logger );
        }
    }

    public static class NeoStoreIdUsage extends NeoStoresDiagnostics
    {

        NeoStoreIdUsage( NeoStores neoStores )
        {
            super( neoStores, "Id usage" );
        }

        @Override
        protected void dump( NeoStores neoStores, Logger logger )
        {
            neoStores.logIdUsage( logger );
        }
    }

    public static class NeoStoreRecords extends NeoStoresDiagnostics
    {
        NeoStoreRecords( NeoStores neoStores )
        {
            super( neoStores,  "Neostore records"  );
        }

        @Override
        protected void dump( NeoStores neoStores, Logger logger )
        {
            neoStores.getMetaDataStore().logRecords( logger );
        }
    }

    private final NeoStores neoStores;

    NeoStoresDiagnostics( NeoStores neoStores, String message )
    {
        super( message );
        this.neoStores = neoStores;
    }

    @Override
    public void dump( Logger logger )
    {
        try
        {
            dump( neoStores, logger );
        }
        catch ( RuntimeException e )
        {
            if ( THROW_NEOSTORE_DIAGNOSTICS_ERRORS )
            {
                throw e;
            }

            logger.log( "Diagnostics not available: " + e.getMessage() );
        }
    }

    protected abstract void dump( NeoStores neoStores, Logger logger );
}
