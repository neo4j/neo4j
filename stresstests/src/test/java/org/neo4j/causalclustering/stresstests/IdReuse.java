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
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helper.Workload;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.id.IdContainer;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.stresstests.TxHelp.isInterrupted;
import static org.neo4j.causalclustering.stresstests.TxHelp.isTransient;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;

/**
 * Resources for stress testing ID-reuse scenarios.
 */
class IdReuse
{
    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName( "testType" );

    /**
     * Validate free ids. All must be unique.
     */
    static class UniqueFreeIds extends Validation
    {
        private final Cluster cluster;
        private final FileSystemAbstraction fs;
        private final Log log;

        UniqueFreeIds( Resources resources )
        {
            super();
            this.cluster = resources.cluster();
            this.fs = resources.fileSystem();
            this.log = resources.logProvider().getLog( getClass() );
        }

        @Override
        protected void validate()
        {
            Iterable<ClusterMember> members = Iterables.concat( cluster.coreMembers(), cluster.readReplicas() );
            Set<Long> unusedIds = new HashSet<>();
            Set<Long> nonUniqueIds = new HashSet<>();

            for ( ClusterMember member : members )
            {
                visitAllIds( member, id ->
                {
                    if ( !unusedIds.add( id ) )
                    {
                        nonUniqueIds.add( id );
                    }
                } );
            }

            if ( nonUniqueIds.size() != 0 )
            {
                for ( ClusterMember member : members )
                {
                    visitAllIds( member, id ->
                    {
                        if ( nonUniqueIds.contains( id ) )
                        {
                            log.error( member + " has non-unique free ID: " + id );
                        }
                    } );
                }

                throw new IllegalStateException( "Non-unique IDs found: " + nonUniqueIds );
            }

            log.info( "Total of " + unusedIds.size() + " reusable ids found" );
        }

        void visitAllIds( ClusterMember member, Consumer<Long> idConsumer )
        {
            String storeDir = member.storeDir().getAbsolutePath();
            File idFile = new File( storeDir, MetaDataStore.DEFAULT_NAME + NODE_STORE_NAME + ".id" );
            IdContainer idContainer = new IdContainer( fs, idFile, 1024, true );
            idContainer.init();
            log.info( idFile.getAbsolutePath() + " has " + idContainer.getFreeIdCount() + " free ids" );

            long id = idContainer.getReusableId();
            while ( id != IdContainer.NO_RESULT )
            {
                idConsumer.accept( id );
                id = idContainer.getReusableId();
            }

            idContainer.close( 0 );
        }
    }

    static class IdReuseSetup extends Preparation
    {
        private final Cluster cluster;

        IdReuseSetup( Resources resources )
        {
            super();
            cluster = resources.cluster();
        }

        @Override
        protected void prepare() throws Exception
        {
            for ( int i = 0; i < 1_000; i++ )
            {
                try
                {
                    cluster.coreTx( ( db, tx ) -> {
                        for ( int j = 0; j < 1_000; j++ )
                        {
                            Node start = db.createNode();
                            Node end = db.createNode();
                            start.createRelationshipTo( end, RELATIONSHIP_TYPE );
                        }
                        tx.success();
                    } );
                }
                catch ( WriteOperationsNotAllowedException e )
                {
                    // skip
                }
            }
        }
    }

    static class InsertionWorkload extends Workload
    {
        private Cluster cluster;

        InsertionWorkload( Control control, Resources resources )
        {
            super( control );
            this.cluster = resources.cluster();
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) -> {
                    Node nodeStart = db.createNode();
                    Node nodeEnd = db.createNode();
                    nodeStart.createRelationshipTo( nodeEnd, RELATIONSHIP_TYPE );
                    tx.success();
                } );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "InsertionWorkload", e );
            }
        }
    }

    static class ReelectionWorkload extends Workload
    {
        private final long reelectIntervalSeconds;
        private final Log log;
        private Cluster cluster;

        ReelectionWorkload( Control control, Resources resources, Config config )
        {
            super( control );
            this.cluster = resources.cluster();
            this.reelectIntervalSeconds = config.reelectIntervalSeconds();
            this.log = config.logProvider().getLog( getClass() );
        }

        @Override
        protected void doWork()
        {
            try
            {
                CoreClusterMember leader = cluster.awaitLeader();
                leader.shutdown();
                leader.start();
                log.info( "Restarting leader" );
                TimeUnit.SECONDS.sleep( reelectIntervalSeconds );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "ReelectionWorkload", e );
            }
        }
    }

    static class DeletionWorkload extends Workload
    {
        private final SecureRandom rnd = new SecureRandom();
        private final int idHighRange;
        private Cluster cluster;

        DeletionWorkload( Control control, Resources resources )
        {
            super( control );
            this.cluster = resources.cluster();
            this.idHighRange = 2_000_000;
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) -> {
                    Node node = db.getNodeById( rnd.nextInt( idHighRange ) );
                    Iterables.stream( node.getRelationships() ).forEach( Relationship::delete );
                    node.delete();

                    tx.success();
                } );
            }
            catch ( NotFoundException e )
            {
                // Expected
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "DeletionWorkload", e );
            }
        }
    }
}
