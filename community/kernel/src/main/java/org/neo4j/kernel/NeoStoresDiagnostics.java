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
package org.neo4j.kernel;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.logging.Logger;

public enum NeoStoresDiagnostics implements DiagnosticsExtractor<NeoStores>
{
    NEO_STORE_VERSIONS( "Store versions:" )
    {
        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.logVersions( logger );
        }
    },
    NEO_STORE_ID_USAGE( "Id usage:" )
    {
        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.logIdUsage( logger );
        }
    },
    NEO_STORE_RECORDS( "Neostore records:" )
    {
        @Override
        void dump( NeoStores source, Logger logger )
        {
            source.getMetaDataStore().logRecords( logger );
        }
    };

    private final String message;

    NeoStoresDiagnostics( String message )
    {
        this.message = message;
    }

    @Override
    public void dumpDiagnostics( final NeoStores source, DiagnosticsPhase phase, Logger logger )
    {
        if ( applicable( phase ) )
        {
            logger.log( message );
            dump( source, logger );
        }
    }

    boolean applicable( DiagnosticsPhase phase )
    {
        return phase.isInitialization() || phase.isExplicitlyRequested();
    }

    abstract void dump( NeoStores source, Logger logger );
}
