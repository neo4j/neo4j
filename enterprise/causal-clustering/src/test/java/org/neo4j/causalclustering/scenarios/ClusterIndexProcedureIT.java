/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;

public class ClusterIndexProcedureIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 3 )
                    .withTimeout( 1000, SECONDS );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void createIndexProcedureMustPropagate() throws Exception
    {
        // create an index
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createIndex( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // node creation is guaranteed to happen after index creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // now the indexes must exist, so we wait for them to come online
        cluster.coreMembers().forEach( this::awaitIndexOnline );
        cluster.readReplicas().forEach( this::awaitIndexOnline );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.database() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.database() ) );
    }

    @Test
    public void createUniquePropertyConstraintMustPropagate() throws Exception
    {
        // create a constraint
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createUniquePropertyConstraint( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // node creation is guaranteed to happen after constraint creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.database() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.database() ) );

        // verify constraints
        cluster.coreMembers().forEach( core -> verifyConstraints( core.database(), UNIQUENESS ) );
        cluster.readReplicas().forEach( rr -> verifyConstraints( rr.database(), UNIQUENESS ) );
    }

    @Test
    public void createNodeKeyConstraintMustPropagate() throws Exception
    {
        // create a node key
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL db.createNodeKey( \":Person(name)\", \"lucene+native-1.0\")" ).close();
            tx.success();
        } );

        // node created just to be able to use dataMatchesEventually as a barrier
        cluster.coreTx( ( db, tx ) ->
        {
            Node person = db.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "Bo Burnham" );
            tx.success();
        } );

        // node creation is guaranteed to happen after constraint creation
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );

        // verify indexes
        cluster.coreMembers().forEach( core -> verifyIndexes( core.database() ) );
        cluster.readReplicas().forEach( rr -> verifyIndexes( rr.database() ) );

        // verify node keys
        cluster.coreMembers().forEach( core -> verifyConstraints( core.database(), NODE_KEY ) );
        cluster.readReplicas().forEach( rr -> verifyConstraints( rr.database(), NODE_KEY ) );
    }

    private void awaitIndexOnline( ClusterMember member )
    {
        GraphDatabaseAPI db = member.database();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private void verifyIndexes( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            // only one index
            Iterator<IndexDefinition> indexes = db.schema().getIndexes().iterator();
            assertTrue( "has one index", indexes.hasNext() );
            IndexDefinition indexDefinition = indexes.next();
            assertFalse( "not more than one index", indexes.hasNext() );

            Label label = indexDefinition.getLabel();
            String property = indexDefinition.getPropertyKeys().iterator().next();

            // with correct pattern and provider
            assertEquals( "correct label", "Person", label.name() );
            assertEquals( "correct property", "name", property );
            assertCorrectProvider( db, label, property );

            tx.success();
        }
    }

    private void verifyConstraints( GraphDatabaseAPI db, ConstraintType expectedConstraintType )
    {
        try ( Transaction tx = db.beginTx() )
        {
            // only one index
            Iterator<ConstraintDefinition> constraints = db.schema().getConstraints().iterator();
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
        CapableIndexReference index = schemaRead.index( labelId, propId );
        assertEquals( "correct provider key", "lucene+native", index.providerKey() );
        assertEquals( "correct provider version", "1.0", index.providerVersion() );
    }
}
