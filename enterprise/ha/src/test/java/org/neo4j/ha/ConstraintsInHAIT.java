/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ConstraintsInHAIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void creatingConstraintOnSlaveIsNotAllowed() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        slave.beginTx();
        try
        {
            ConstraintCreator constraintCreator = slave.schema()
                    .constraintFor( DynamicLabel.label( "LabelName" ) ).assertPropertyIsUnique( "PropertyName" );

            // when
            constraintCreator.create();
            fail( "should have thrown exception" );
        }
        catch ( InvalidTransactionTypeException e )
        {
            assertThat(e.getMessage(), equalTo("Modifying the database schema can only be done on the master server, " +
                    "this server is a slave. Please issue schema modification commands directly to the master."));
        }
    }
}
