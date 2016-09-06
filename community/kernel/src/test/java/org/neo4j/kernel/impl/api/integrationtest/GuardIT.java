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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.guard.TimeoutGuard;
import org.neo4j.kernel.impl.api.GuardingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationContainer;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class GuardIT
{
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    @Test
    public void useTimeoutGuard() throws Exception
    {
        GraphDatabaseAPI database = startDataBase();

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Guard guard = dependencyResolver.resolveDependency( Guard.class );
        assertThat( guard, instanceOf( TimeoutGuard.class ) );
    }

    @Test
    public void includeGuardingOperationLayerOnGuardingParts() throws Exception
    {
        GraphDatabaseAPI database = startDataBase();

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        StatementOperationContainer operationParts = dependencyResolver.resolveDependency( StatementOperationContainer.class );
        StatementOperationParts guardedParts = operationParts.guardedParts();
        assertThat( guardedParts.entityReadOperations(), instanceOf( GuardingStatementOperations.class ) );
        assertThat( guardedParts.entityWriteOperations(), instanceOf( GuardingStatementOperations.class ) );
    }

    @Test
    public void notIncludeGuardingOperationLayerOnNonGuardingParts() throws Exception
    {
        GraphDatabaseAPI database = startDataBase();

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        StatementOperationContainer operationParts = dependencyResolver.resolveDependency( StatementOperationContainer.class );
        StatementOperationParts guardedParts = operationParts.nonGuarderParts();
        assertThat( guardedParts.entityReadOperations(), not( instanceOf( GuardingStatementOperations.class ) ) );
        assertThat( guardedParts.entityWriteOperations(), not( instanceOf( GuardingStatementOperations.class ) ) );
    }

    private GraphDatabaseAPI startDataBase()
    {
        GraphDatabaseAPI database =
                (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        cleanupRule.add( database );
        return database;
    }
}
