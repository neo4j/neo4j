/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
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
    public ClusterRule clusterRule = new ClusterRule();

    @Test
    public void creatingConstraintOnSlaveIsNotAllowed()
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        slave.beginTx();
        try
        {
            ConstraintCreator constraintCreator = slave.schema()
                    .constraintFor( Label.label( "LabelName" ) ).assertPropertyIsUnique( "PropertyName" );

            // when
            constraintCreator.create();
            fail( "should have thrown exception" );
        }
        catch ( ConstraintViolationException e )
        {
            assertThat(e.getMessage(), equalTo("Modifying the database schema can only be done on the master server, " +
                    "this server is a slave. Please issue schema modification commands directly to the master."));
        }
    }
}
