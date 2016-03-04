/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import org.neo4j.consistency.checking.full.DetectAllRelationshipInconsistenciesIT;
import org.neo4j.consistency.checking.full.ExecutionOrderIntegrationTest;
import org.neo4j.consistency.checking.full.FullCheckIntegrationTest;
import org.neo4j.consistency.repair.RelationshipChainExplorerTest;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

@RunWith( Suite.class )
@Suite.SuiteClasses( {
        DetectAllRelationshipInconsistenciesIT.class,
        ExecutionOrderIntegrationTest.class,
        FullCheckIntegrationTest.class,
        RelationshipChainExplorerTest.class,
        ConsistencyCheckServiceIntegrationTest.class
} )
public class HighLimitConsistencyCheckIT
{
    @BeforeClass
    public static void enableTestHighLimitFormat()
    {
        HighLimitWithSmallRecords.enable();
    }

    @AfterClass
    public static void disableTestHighLimitFormat()
    {
        HighLimitWithSmallRecords.disable();
    }
}
