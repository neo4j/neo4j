/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.systemgraph;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.TopologyGraphDbmsModel;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_CREATED_AT_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_DESIGNATED_SEEDER_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_INITIAL_SERVERS_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STORAGE_ENGINE_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STORE_CREATION_TIME_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STORE_RANDOM_ID_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STORE_VERSION_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_UPDATE_ID_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DELETED_DATABASE_DUMP_DATA_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DELETED_DATABASE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.HOSTED_ON_BOOTSTRAPPER_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.HOSTED_ON_MODE_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.HOSTED_ON_RELATIONSHIP;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.INSTANCE_DISCOVERED_AT_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.INSTANCE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.INSTANCE_MODE_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.INSTANCE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.PRIMARY_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.REMOVED_INSTANCE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.TARGET_NAME_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.URL_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.WAS_HOSTED_ON_RELATIONSHIP;

@ImpermanentDbmsExtension
public abstract class BaseTopologyGraphDbmsModelTest
{
    @Inject
    protected DatabaseManagementService managementService;
    @Inject
    protected GraphDatabaseService db;

    protected Transaction tx;

    @BeforeEach
    public void before()
    {
        tx = db.beginTx();
        createModel( tx );
    }

    @AfterEach
    public void after()
    {
        tx.commit();
        tx.close();
    }

    protected abstract void createModel( Transaction tx );

    protected ServerId newInstance( Consumer<InstanceNodeBuilder> setup )
    {
        return newInstance( setup, false );
    }

    protected ServerId newRemovedInstance( Consumer<InstanceNodeBuilder> setup )
    {
        return newInstance( setup, true );
    }

    protected NamedDatabaseId newDatabase( Consumer<DatabaseNodeBuilder> setup )
    {
        return newDatabase( setup, false );
    }

    protected NamedDatabaseId newDeletedDatabase( Consumer<DatabaseNodeBuilder> setup )
    {
        return newDatabase( setup, true );
    }

    public static ServerId serverId( int seed )
    {
        var rng = new Random( seed );
        return new ServerId( new UUID( rng.nextLong(), rng.nextLong() ) );
    }

    public static Set<ServerId> serverIds( int from, int until )
    {
        return IntStream.range( from, until ).mapToObj( BaseTopologyGraphDbmsModelTest::serverId ).collect( Collectors.toSet() );
    }

    protected void connect( NamedDatabaseId databaseId, ServerId serverId, TopologyGraphDbmsModel.HostedOnMode mode, boolean bootstrapper, boolean was )
    {
        try ( var tx = db.beginTx() )
        {
            var database = findDatabase( databaseId, tx );
            var instance = findInstance( serverId, tx );
            var relationship = mergeHostOn( was, database, instance );
            relationship.setProperty( HOSTED_ON_MODE_PROPERTY, mode.name() );
            if ( bootstrapper )
            {
                relationship.setProperty( HOSTED_ON_BOOTSTRAPPER_PROPERTY, true );
            }
            tx.commit();
        }
    }
    private Relationship mergeHostOn( boolean wasHostedOn, Node database, Node instance )
    {
        StreamSupport.stream( database.getRelationships( Direction.OUTGOING, HOSTED_ON_RELATIONSHIP, WAS_HOSTED_ON_RELATIONSHIP ).spliterator(), false )
                .filter( rel -> Objects.equals( rel.getEndNode(), instance ) )
                .forEach( Relationship::delete );
        var nextRelLabel = wasHostedOn ? WAS_HOSTED_ON_RELATIONSHIP : HOSTED_ON_RELATIONSHIP;
        return database.createRelationshipTo( instance, nextRelLabel );
    }

    protected void disconnect( NamedDatabaseId databaseId, ServerId serverId, boolean replaceWithWas )
    {
        try ( var tx = db.beginTx() )
        {
            var database = findDatabase( databaseId, tx );
            var instance = findInstance( serverId, tx );

            StreamSupport.stream( database.getRelationships( HOSTED_ON_RELATIONSHIP ).spliterator(), false )
                    .filter( rel -> rel.getEndNode().equals( instance ) )
                    .forEach( rel ->
                    {
                        if ( replaceWithWas )
                        {
                            var was = database.createRelationshipTo( instance, WAS_HOSTED_ON_RELATIONSHIP );
                            was.setProperty( HOSTED_ON_MODE_PROPERTY, rel.getProperty( HOSTED_ON_MODE_PROPERTY ) );
                        }
                        rel.delete();
                    } );
            tx.commit();
        }
    }

    protected void databaseDelete( NamedDatabaseId id )
    {
        try ( var tx = db.beginTx() )
        {
            var database = findDatabase( id, tx );
            database.setProperty( DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty( DATABASE_UPDATE_ID_PROPERTY )) + 1L );
            database.addLabel( DELETED_DATABASE_LABEL );
            database.removeLabel( DATABASE_LABEL );
            tx.commit();
        }
    }

    protected void databaseSetState( NamedDatabaseId id, TopologyGraphDbmsModel.DatabaseStatus state )
    {
        try ( var tx = db.beginTx() )
        {
            var database = findDatabase( id, tx );
            database.setProperty( DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty( DATABASE_UPDATE_ID_PROPERTY )) + 1L );
            database.setProperty( DATABASE_STATUS_PROPERTY, state.name() );
            tx.commit();
        }
    }

    protected void databaseIncreaseUpdateId( NamedDatabaseId... ids )
    {
        try ( var tx = db.beginTx() )
        {
            for ( NamedDatabaseId id : ids )
            {
                var database = findDatabase( id, tx );
                database.setProperty( DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty( DATABASE_UPDATE_ID_PROPERTY )) + 1L );
            }
            tx.commit();
        }
    }

    private NamedDatabaseId newDatabase( Consumer<DatabaseNodeBuilder> setup, boolean deleted )
    {
        try ( var tx = db.beginTx() )
        {
            var builder = new DatabaseNodeBuilder( tx, deleted );
            setup.accept( builder );
            return builder.commit();
        }
    }

    private ServerId newInstance( Consumer<InstanceNodeBuilder> setup, boolean removed )
    {
        try ( var tx = db.beginTx() )
        {
            var builder = new InstanceNodeBuilder( tx, removed );
            setup.accept( builder );
            return builder.commit();
        }
    }

    private Node findInstance( ServerId serverId, Transaction tx )
    {
        return Optional.ofNullable( tx.findNode( INSTANCE_LABEL, INSTANCE_UUID_PROPERTY, serverId.uuid().toString() ) )
                .orElseGet( () -> tx.findNode( REMOVED_INSTANCE_LABEL, DATABASE_UUID_PROPERTY, serverId.uuid().toString() ) );
    }

    private Node findDatabase( NamedDatabaseId databaseId, Transaction tx )
    {
        return Optional.ofNullable( tx.findNode( DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.databaseId().uuid().toString() ) )
                .orElseGet( () -> tx.findNode( DELETED_DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.databaseId().uuid().toString() ) );
    }

    protected static class DatabaseNodeBuilder
    {
        Transaction tx;
        Node node;

        public DatabaseNodeBuilder( Transaction tx, boolean deleted )
        {
            this.tx = tx;
            node = tx.createNode( deleted ? DELETED_DATABASE_LABEL : DATABASE_LABEL );
        }

        public DatabaseNodeBuilder withDatabase( String databaseName )
        {
            return withDatabase( DatabaseIdFactory.from( databaseName, UUID.randomUUID() ) );
        }

        public DatabaseNodeBuilder withDatabase( NamedDatabaseId namedDatabaseId )
        {
            node.setProperty( DATABASE_NAME_PROPERTY, namedDatabaseId.name() );
            node.setProperty( DATABASE_UUID_PROPERTY, namedDatabaseId.databaseId().uuid().toString() );
            node.setProperty( DATABASE_STATUS_PROPERTY, TopologyGraphDbmsModel.DatabaseStatus.online.name() );
            node.setProperty( DATABASE_UPDATE_ID_PROPERTY, 0L );
            return this;
        }

        public DatabaseNodeBuilder withStorageEngine( String storageEngine )
        {
            node.setProperty( DATABASE_STORAGE_ENGINE_PROPERTY, storageEngine );
            return this;
        }

        public DatabaseNodeBuilder asStopped()
        {
            node.setProperty( DATABASE_STATUS_PROPERTY, TopologyGraphDbmsModel.DatabaseStatus.offline.name() );
            return this;
        }

        public DatabaseNodeBuilder withDump()
        {
            node.setProperty( DELETED_DATABASE_DUMP_DATA_PROPERTY, true );
            return this;
        }

        public DatabaseNodeBuilder withInitialMembers( Set<ServerId> initialMembers )
        {
            node.setProperty( DATABASE_INITIAL_SERVERS_PROPERTY, initialMembers.stream().map( server -> server.uuid().toString() ).toArray( String[]::new ) );
            return this;
        }

        public DatabaseNodeBuilder withStoreId( StoreId storeId )
        {
            node.setProperty( DATABASE_CREATED_AT_PROPERTY,
                    ZonedDateTime.ofInstant( Instant.ofEpochMilli( storeId.getCreationTime() ), ZoneId.systemDefault() ) );
            node.setProperty( DATABASE_STORE_CREATION_TIME_PROPERTY, storeId.getCreationTime() );
            node.setProperty( DATABASE_STORE_RANDOM_ID_PROPERTY, storeId.getRandomId() );
            node.setProperty( DATABASE_STORE_VERSION_PROPERTY, MetaDataStore.versionLongToString( storeId.getStoreVersion() ) );
            return this;
        }

        public DatabaseNodeBuilder withNumbers( int primaries, int secondaries )
        {
            node.setProperty( DATABASE_PRIMARIES_PROPERTY, primaries );
            node.setProperty( DATABASE_SECONDARIES_PROPERTY, secondaries );
            return this;
        }

        public DatabaseNodeBuilder withDesignatedSeeder( ServerId designatedSeeder )
        {
            node.setProperty( DATABASE_DESIGNATED_SEEDER_PROPERTY, designatedSeeder.uuid().toString() );
            return this;
        }

        public NamedDatabaseId commit()
        {
            var id = DatabaseIdFactory.from( (String) node.getProperty( DATABASE_NAME_PROPERTY ),
                    UUID.fromString( (String) node.getProperty( DATABASE_UUID_PROPERTY ) ) );
            tx.commit();
            return id;
        }
    }

    protected static class InstanceNodeBuilder
    {
        Transaction tx;
        Node node;

        public InstanceNodeBuilder( Transaction tx, boolean removed )
        {
            this.tx = tx;
            node = tx.createNode( removed ? REMOVED_INSTANCE_LABEL : INSTANCE_LABEL );
        }

        public InstanceNodeBuilder withInstance()
        {
            return withInstance( new ServerId( UUID.randomUUID() ) );
        }

        public InstanceNodeBuilder withInstance( ServerId serverId )
        {
            node.setProperty( INSTANCE_UUID_PROPERTY, serverId.uuid().toString() );
            node.setProperty( INSTANCE_MODE_PROPERTY, GraphDatabaseSettings.Mode.CORE.name() );
            node.setProperty( INSTANCE_STATUS_PROPERTY, TopologyGraphDbmsModel.InstanceStatus.active.name() );
            return this;
        }

        public InstanceNodeBuilder withMode( GraphDatabaseSettings.Mode mode )
        {
            node.setProperty( INSTANCE_MODE_PROPERTY, mode.name() );
            return this;
        }

        public InstanceNodeBuilder asDraining()
        {
            node.setProperty( INSTANCE_STATUS_PROPERTY, TopologyGraphDbmsModel.InstanceStatus.draining.name() );
            return this;
        }

        public ServerId commit()
        {
            node.setProperty( INSTANCE_DISCOVERED_AT_PROPERTY, ZonedDateTime.now() );
            var id = new ServerId( UUID.fromString( (String) node.getProperty( INSTANCE_UUID_PROPERTY ) ) );
            tx.commit();
            return id;
        }
    }

    protected Node createLocalAliasForDatabase( Transaction tx, String name, boolean primary, NamedDatabaseId databaseId )
    {
        var databaseNode = findDatabase( databaseId, tx );
        var aliasNode = tx.createNode( DATABASE_NAME_LABEL );
        aliasNode.setProperty( PRIMARY_PROPERTY, primary );
        aliasNode.setProperty( DATABASE_NAME_PROPERTY, name );
        aliasNode.createRelationshipTo( databaseNode, TARGETS_RELATIONSHIP );
        return aliasNode;
    }

    protected Node createRemoteAliasForDatabase( Transaction tx, String name, String targetName, RemoteUri uri )
    {
        var aliasNode = tx.createNode( REMOTE_DATABASE_LABEL, DATABASE_NAME_LABEL );
        aliasNode.setProperty( PRIMARY_PROPERTY, false );
        aliasNode.setProperty( DATABASE_NAME_PROPERTY, name );
        aliasNode.setProperty( TARGET_NAME_PROPERTY, targetName );
        var uriString = String.format( "%s://%s", uri.getScheme(), uri.getAddresses().get( 0 ) );
        aliasNode.setProperty( URL_PROPERTY, uriString );
        return aliasNode;
    }

}
