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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Logger;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class NeoStoresDiagnosticsTest
{
    private AssertableLogProvider logProvider;
    private Logger logger;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider();
        logger = logProvider.getLog( DiagnosticsManager.class ).infoLogger();
    }

    @Test
    void mustInfoLogIfIdGeneratorsAreUninitialisedAndDebugLoggingIsOff()
    {
        NeoStores neoStores = mock( NeoStores.class );
        NeoStoresDiagnostics.NeoStoreIdUsage idUsage = new NeoStoresDiagnostics.NeoStoreIdUsage( neoStores );
        String errorMessage = "IdGenerator is not initialized";

        doThrow( new IllegalStateException( errorMessage ) ).when( neoStores ).logIdUsage( logger );

        idUsage.dump( logger );

        AssertableLogProvider.MessageMatcher messageMatcher = logProvider.rawMessageMatcher();
        messageMatcher.assertContains( "Diagnostics not available" );
        messageMatcher.assertContains( errorMessage );
        messageMatcher.assertNotContains( "Exception" );
    }
}
