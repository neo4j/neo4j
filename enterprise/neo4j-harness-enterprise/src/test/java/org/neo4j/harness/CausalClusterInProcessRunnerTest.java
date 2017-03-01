/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.harness;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.harness.CausalClusterInProcessRunner.CausalCluster;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;

public class CausalClusterInProcessRunnerTest
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldBootAndShutdownCluster() throws Exception
    {
        CausalCluster cluster = new CausalCluster( 3, 3, testDirectory.absolutePath().toPath(), NullLogProvider.getInstance() );

        cluster.boot();
        cluster.shutdown();
    }
}
