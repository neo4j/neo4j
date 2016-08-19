/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.guard.EmptyGuard;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.guard.TimeoutGuard;
import org.neo4j.kernel.impl.api.GuardingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class GuardIT
{
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    @Test
    public void timeoutGuardUsedWhenGuardEnabled() throws Exception
    {
        GraphDatabaseAPI database = startDataBase( getEnabledGuardConfigMap() );

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Guard guard = dependencyResolver.resolveDependency( Guard.class );
        assertThat( guard, instanceOf( TimeoutGuard.class ) );
    }

    @Test
    public void emptyGuardUsedWhenGuardDisabled() throws Exception
    {
        GraphDatabaseAPI database = startDataBase( getDisabledGuardConfigMap() );

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Guard guard = dependencyResolver.resolveDependency( Guard.class );
        assertThat( guard, instanceOf( EmptyGuard.class ) );
    }

    @Test
    public void includeGuardingOperationLayerWhenGuardEnabled() throws Exception
    {
        GraphDatabaseAPI database = startDataBase( getEnabledGuardConfigMap() );

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        StatementOperationParts operationParts =
                dependencyResolver.resolveDependency( StatementOperationParts.class );
        assertThat( operationParts.entityReadOperations(), instanceOf( GuardingStatementOperations.class ) );
        assertThat( operationParts.entityWriteOperations(), instanceOf( GuardingStatementOperations.class ) );
    }

    @Test
    public void noGuardingOperationLayerWhenGuardDisabled() throws Exception
    {
        GraphDatabaseAPI database = startDataBase( getDisabledGuardConfigMap() );

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        StatementOperationParts operationParts =
                dependencyResolver.resolveDependency( StatementOperationParts.class );
        assertThat( operationParts.entityReadOperations(), not( instanceOf( GuardingStatementOperations.class ) ) );
        assertThat( operationParts.entityWriteOperations(), not( instanceOf( GuardingStatementOperations.class ) ) );
    }

    private GraphDatabaseAPI startDataBase( Map<Setting<?>,String> disabledGuardConfigMap )
    {
        GraphDatabaseAPI database =
                (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase( disabledGuardConfigMap );
        cleanupRule.add( database );
        return database;
    }

    private Map<Setting<?>,String> getEnabledGuardConfigMap()
    {
        return genericMap( GraphDatabaseSettings.execution_guard_enabled, Settings.TRUE );
    }

    private Map<Setting<?>,String> getDisabledGuardConfigMap()
    {
        return genericMap( GraphDatabaseSettings.execution_guard_enabled, Settings.FALSE );
    }
}
