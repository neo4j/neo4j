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
package org.neo4j.graphdb;

import org.junit.Test;

import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.GlobalGraphOperationsFacadeMethods.ALL_GLOBAL_GRAPH_OPERATIONS_FACADE_METHODS;

public class MandatoryTransactionsForGlobalGraphOperations
    extends AbstractMandatoryTransactionsTest<GlobalGraphOperations>
{
    @Test
    public void shouldRequireTransactionsWhenCallingGlobalGraphOperations() throws Exception
    {
        assertFacadeMethodsThrowNotInTransaction( obtainEntity(), ALL_GLOBAL_GRAPH_OPERATIONS_FACADE_METHODS );
    }

    @Test
    public void shouldTerminateWhenCallingGlobalGraphOperations() throws Exception
    {
        assertFacadeMethodsThrowAfterTerminate( ALL_GLOBAL_GRAPH_OPERATIONS_FACADE_METHODS );
    }

    @Override
    protected GlobalGraphOperations obtainEntityInTransaction( GraphDatabaseService graphDatabaseService )
    {
        return GlobalGraphOperations.at( graphDatabaseService );
    }
}
