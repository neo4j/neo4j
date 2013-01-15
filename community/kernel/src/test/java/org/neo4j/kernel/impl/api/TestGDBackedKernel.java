/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.test.ImpermanentDatabaseRule;

public class TestGDBackedKernel
{

    public final @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldCreateTransactionContext() throws Exception
    {
        // Given
        KernelAPI kernel = new GDBackedKernel( dbRule.getGraphDatabaseAPI() );

        // When
        TransactionContext tx = kernel.newTransactionContext();

        // Then
        assertThat(tx, not(nullValue()));
    }
}
