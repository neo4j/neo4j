/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.database;

import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.Logger;

public abstract class NeoStoresDiagnostics implements DiagnosticsProvider
{
    public static class NeoStoreVersions extends NeoStoresDiagnostics
    {
        public NeoStoreVersions( NeoStores nodeStores )
        {
            super( nodeStores, "Store versions:" );
        }

        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.logVersions( logger );
        }
    }

    public static class NeoStoreIdUsage extends NeoStoresDiagnostics
    {

        public NeoStoreIdUsage( NeoStores neoStores )
        {
            super( neoStores, "Id usage:" );
        }

        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.logIdUsage( logger );
        }
    }

    public static class NeoStoreRecords extends NeoStoresDiagnostics
    {
        public NeoStoreRecords( NeoStores neoStores )
        {
            super( neoStores,  "Neostore records:"  );
        }

        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.getMetaDataStore().logRecords( logger );
        }
    }

    private final String message;
    private final NeoStores neoStores;

    NeoStoresDiagnostics( NeoStores neoStores, String message )
    {
        this.neoStores = neoStores;
        this.message = message;
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return null;
    }

    @Override
    public void dump( Logger logger )
    {
        logger.log( message );
        dump( neoStores, logger );
    }

    abstract void dump( NeoStores source, Logger logger );
}
