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
package org.neo4j.fabric;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.planning.QueryType;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.virtual.MapValue;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( {DefaultFileSystemExtension.class, TestDirectorySupportExtension.class} )
public class SecurityProcedureQueryTypeTest
{
    @Inject
    static TestDirectory testDirectory;

    private static FabricPlanner planner;
    private static DatabaseManagementService databaseManagementService;

    @BeforeAll
    static void beforeAll()
    {
        databaseManagementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
        DependencyResolver dependencyResolver = ((GraphDatabaseFacade) databaseManagementService.database( "system" )).getDependencyResolver();
        planner = dependencyResolver.resolveDependency( FabricPlanner.class );
    }

    @AfterAll
    static void afterAll()
    {
        databaseManagementService.shutdown();
    }

    @ParameterizedTest( name = "statement {0} should have queryType {1}" )
    @MethodSource( "procedures" )
    void securityProcedure( String statement, QueryType queryType )
    {
        var instance = planner.instance( statement, MapValue.EMPTY, "system" );
        assertThat( instance.plan().queryType() ).isEqualTo( queryType );
    }

    static List<Arguments> procedures()
    {
        return List.of(
                Arguments.of( "CALL dbms.security.listUsers()", QueryType.READ() ),
                Arguments.of( "CALL dbms.security.createUser('Tobias', 'secret')", QueryType.WRITE() ),
                Arguments.of( "CALL dbms.security.changePassword('newSecret')", QueryType.WRITE() ),
                Arguments.of( "CALL dbms.security.deleteUser('Tobias')", QueryType.WRITE() ),
                Arguments.of( "CALL dbms.showCurrentUser()", QueryType.READ() ) // DBMS mode gives READ
        );
    }
}
