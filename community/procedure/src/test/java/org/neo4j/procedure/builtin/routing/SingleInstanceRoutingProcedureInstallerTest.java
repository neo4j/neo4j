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
package org.neo4j.procedure.builtin.routing;

import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static java.util.stream.Collectors.toSet;
import static org.eclipse.collections.impl.set.mutable.UnifiedSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SingleInstanceRoutingProcedureInstallerTest
{
    @Test
    void shouldRegisterRoutingProcedures() throws Exception
    {
        var databaseReferenceRepo = mock( DatabaseReferenceRepository.class );
        var databaseAvailabilityChecker = mock( DatabaseAvailabilityChecker.class );
        var portRegister = mock( ConnectorPortRegister.class );
        var clientRoutingDomainChecker = mock( ClientRoutingDomainChecker.class );
        var config = Config.defaults();
        var logProvider = NullLogProvider.getInstance();

        var installer = new SingleInstanceRoutingProcedureInstaller( databaseAvailabilityChecker, clientRoutingDomainChecker,
                                                                     portRegister, config, logProvider, databaseReferenceRepo );
        var procedures = spy( new GlobalProceduresRegistry() );

        installer.install( procedures );

        verify( procedures, times( 2 ) ).register( any( GetRoutingTableProcedure.class ) );

        var expectedNames = newSetWith(
                new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ),
                new QualifiedName( new String[]{"dbms", "cluster", "routing"}, "getRoutingTable" ) );

        var actualNames = procedures.getAllProcedures().stream()
                .map( ProcedureSignature::name )
                .collect( toSet() );

        assertEquals( expectedNames, actualNames );
    }
}
