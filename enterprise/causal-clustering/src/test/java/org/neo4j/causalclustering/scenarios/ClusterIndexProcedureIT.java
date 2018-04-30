/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterIndexProcedureIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 3 )
                    .withTimeout( 1000, SECONDS );

    private Cluster<?> cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void createIndexProcedureMustPropagate() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // when
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createIndex( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );
        awaitIndexOnline( leader );

        // then
        Cluster.dataMatchesEventually( leader, cluster.coreMembers() );
        Cluster.dataMatchesEventually( leader, cluster.readReplicas() );
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            verifyIndexes( core.database() );
        }
        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            verifyIndexes( readReplica.database() );
        }
    }

    @Test
    public void createUniquePropertyConstraintMustPropagate() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // when
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createUniquePropertyConstraint( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );

        // then
        Cluster.dataMatchesEventually( leader, cluster.coreMembers() );
        Cluster.dataMatchesEventually( leader, cluster.readReplicas() );
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            verifyIndexes( core.database() );
            verifyConstraints( core.database(), ConstraintType.UNIQUENESS );
        }
        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            verifyIndexes( readReplica.database() );
            verifyConstraints( readReplica.database(), ConstraintType.UNIQUENESS );
        }
    }

    @Test
    public void createNodeKeyConstraintMustPropagate() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // when
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createNodeKey( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );

        // then
        Cluster.dataMatchesEventually( leader, cluster.coreMembers() );
        Cluster.dataMatchesEventually( leader, cluster.readReplicas() );
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            verifyIndexes( core.database() );
            verifyConstraints( core.database(), ConstraintType.NODE_KEY );
        }
        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            verifyIndexes( readReplica.database() );
            verifyConstraints( readReplica.database(), ConstraintType.NODE_KEY );
        }
    }

    private void awaitIndexOnline( CoreClusterMember member )
    {
        CoreGraphDatabase db = member.database();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private void verifyIndexes( GraphDatabaseFacade database )
    {
        try ( Transaction tx = database.beginTx() )
        {
            // only one index
            Iterator<IndexDefinition> indexes = database.schema().getIndexes().iterator();
            assertTrue( "has one index", indexes.hasNext() );
            IndexDefinition indexDefinition = indexes.next();
            assertFalse( "not more than one index", indexes.hasNext() );

            Label label = indexDefinition.getLabel();
            String property = indexDefinition.getPropertyKeys().iterator().next();

            // with correct pattern and provider
            assertEquals( "correct label", "Person", label.name() );
            assertEquals( "correct property", "name", property );
            assertCorrectProvider( database, label, property );

            tx.success();
        }
    }

    private void verifyConstraints( GraphDatabaseFacade database, ConstraintType expectedConstraintType )
    {
        try ( Transaction tx = database.beginTx() )
        {
            // only one index
            Iterator<ConstraintDefinition> constraints = database.schema().getConstraints().iterator();
            assertTrue( "has one index", constraints.hasNext() );
            ConstraintDefinition constraint = constraints.next();
            assertFalse( "not more than one index", constraints.hasNext() );

            Label label = constraint.getLabel();
            String property = constraint.getPropertyKeys().iterator().next();
            ConstraintType constraintType = constraint.getConstraintType();

            // with correct pattern and provider
            assertEquals( "correct label", "Person", label.name() );
            assertEquals( "correct property", "name", property );
            assertEquals( "correct constraint type", expectedConstraintType, constraintType );

            tx.success();
        }
    }

    private void assertCorrectProvider( GraphDatabaseAPI db, Label label, String property )
    {
        KernelTransaction kernelTransaction =
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( false );
        TokenRead tokenRead = kernelTransaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        int propId = tokenRead.propertyKey( property );
        SchemaRead schemaRead = kernelTransaction.schemaRead();
        IndexReference index = schemaRead.index( labelId, propId );
        assertEquals( "correct provider key", "lucene+native", index.providerKey() );
        assertEquals( "correct provider version", "1.0", index.providerVersion() );
    }
}
