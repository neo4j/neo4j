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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.Level;
import org.neo4j.values.storable.DurationValue;

import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_MAX_LIFETIME;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_ACQUISITION_TIMEOUT;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_IDLE_TEST;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_MAX_SIZE;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_TIMEOUT;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.LOGGING_LEVEL;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.SSL_ENABLED;

public class CommunityTopologyGraphDbmsModel implements TopologyGraphDbmsModel
{
    protected final Transaction tx;

    public CommunityTopologyGraphDbmsModel( Transaction tx )
    {
        this.tx = tx;
    }

    public Map<NamedDatabaseId,TopologyGraphDbmsModel.DatabaseAccess> getAllDatabaseAccess()
    {
        return tx.findNodes( DATABASE_LABEL ).stream()
                 .collect( Collectors.toMap( CommunityTopologyGraphDbmsModel::getDatabaseId,
                                             CommunityTopologyGraphDbmsModel::getDatabaseAccess ) );
    }

    private static TopologyGraphDbmsModel.DatabaseAccess getDatabaseAccess( Node databaseNode )
    {
        var accessString = (String) databaseNode.getProperty( DATABASE_ACCESS_PROPERTY, TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE.toString() );
        return Enum.valueOf( TopologyGraphDbmsModel.DatabaseAccess.class, accessString );
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByAlias( String databaseName )
    {
        return getDatabaseIdByAlias0( databaseName )
                .or( () -> getDatabaseIdBy( DATABASE_NAME_PROPERTY, databaseName ) );
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByUUID( UUID uuid )
    {
        return getDatabaseIdBy( DATABASE_UUID_PROPERTY, uuid.toString() );
    }

    @Override
    public Set<NamedDatabaseId> getAllDatabaseIds()
    {
        return tx.findNodes( DATABASE_LABEL ).stream()
                     .map( CommunityTopologyGraphDbmsModel::getDatabaseId )
                     .collect( Collectors.toUnmodifiableSet() );
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences()
    {
        var primaryRefs = getAllDatabaseIds().stream().map( this::primaryRefFromDatabaseId );
        var internalAliasRefs = getAllInternalDatabaseReferences0();
        var internalRefs = Stream.concat( primaryRefs, internalAliasRefs );
        var externalRefs = getAllExternalDatabaseReferences0();

        return Stream.concat( internalRefs, externalRefs ).collect( Collectors.toUnmodifiableSet() );
    }

    @Override
    public Set<DatabaseReference.Internal> getAllInternalDatabaseReferences()
    {
        var primaryRefs = getAllDatabaseIds().stream().map( this::primaryRefFromDatabaseId );
        var localAliasRefs = getAllInternalDatabaseReferences0();

        return Stream.concat( primaryRefs, localAliasRefs ).collect( Collectors.toUnmodifiableSet() );
    }

    private DatabaseReference.Internal primaryRefFromDatabaseId( NamedDatabaseId databaseId )
    {
        var alias = new NormalizedDatabaseName( databaseId.name() );
        return new DatabaseReference.Internal( alias, databaseId );
    }

    private Stream<DatabaseReference.Internal> getAllInternalDatabaseReferences0()
    {
        return tx.findNodes( DATABASE_NAME_LABEL ).stream()
                 .flatMap( alias -> getTargetedDatabase( alias )
                         .flatMap( db -> createInternalReference( alias, db ) ).stream() );
    }

    private Optional<DatabaseReference.Internal> createInternalReference( Node alias, NamedDatabaseId targetedDatabase )
    {
        return ignoreConcurrentDeletes( () ->
        {
            var aliasName = new NormalizedDatabaseName( getPropertyOnNode( DATABASE_NAME, alias, NAME_PROPERTY, String.class ) );
            return Optional.of( new DatabaseReference.Internal( aliasName, targetedDatabase ) );
        } );
    }

    @Override
    public Set<DatabaseReference.External> getAllExternalDatabaseReferences()
    {
        return getAllExternalDatabaseReferences0().collect( Collectors.toUnmodifiableSet() );
    }

    private Stream<DatabaseReference.External> getAllExternalDatabaseReferences0()
    {
        return tx.findNodes( REMOTE_DATABASE_LABEL ).stream().flatMap( alias -> createExternalReference( alias ).stream() );
    }

    private Optional<DatabaseReference.External> createExternalReference( Node ref )
    {
        return ignoreConcurrentDeletes( () ->
        {
            var uriString = getPropertyOnNode( REMOTE_DATABASE_LABEL_DESCRIPTION, ref, URL_PROPERTY, String.class );
            var targetName = new NormalizedDatabaseName( getPropertyOnNode( REMOTE_DATABASE_LABEL_DESCRIPTION, ref, TARGET_NAME_PROPERTY, String.class ) );
            var aliasName = new NormalizedDatabaseName( getPropertyOnNode( REMOTE_DATABASE_LABEL_DESCRIPTION, ref, NAME_PROPERTY, String.class ) );

            var uri = URI.create( uriString );
            var host = SocketAddressParser.socketAddress( uri, BoltConnector.DEFAULT_PORT, SocketAddress::new );
            var remoteUri = new RemoteUri( uri.getScheme(), List.of( host ), uri.getQuery() );
            var uuid = getPropertyOnNode( REMOTE_DATABASE_LABEL_DESCRIPTION, ref, VERSION_PROPERTY, String.class );
            return Optional.of( new DatabaseReference.External( targetName, aliasName, remoteUri, UUID.fromString( uuid ) ) );
        } );
    }

    @Override
    public Optional<DatabaseReference> getDatabaseRefByAlias( String databaseName )
    {
        // A uniqueness constraint at the Cypher level should prevent two references from ever having the same name, but
        // in case they do, we simply prefer the internal reference.
        return getInternalDatabaseReference( databaseName )
                .or( () -> getExternalDatabaseReference( databaseName ) );
    }

    @Override
    public Optional<DriverSettings> getDriverSettings( String databaseName )
    {
        return Optional.ofNullable( tx.findNode( REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName ) )
                       .flatMap( CommunityTopologyGraphDbmsModel::getDriverSettings );
    }

    @Override
    public Optional<ExternalDatabaseCredentials> getExternalDatabaseCredentials( String databaseName )
    {
        return Optional.ofNullable( tx.findNode( REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName ) )
                       .flatMap( CommunityTopologyGraphDbmsModel::getDatabaseCredentials );
    }

    private Optional<DatabaseReference> getInternalDatabaseReference( String databaseName )
    {
        var aliasNode = Optional.ofNullable( tx.findNode( DATABASE_NAME_LABEL, NAME_PROPERTY, databaseName ) );
        return aliasNode.flatMap( alias -> getTargetedDatabase( alias )
                .flatMap( db -> createInternalReference( alias, db ) ) );
    }

    private Optional<DatabaseReference> getExternalDatabaseReference( String databaseName )
    {
        return Optional.ofNullable( tx.findNode( REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName ) )
                       .flatMap( this::createExternalReference );
    }

    private Optional<NamedDatabaseId> getDatabaseIdByAlias0( String databaseName )
    {
        var node = Optional.ofNullable( tx.findNode( DATABASE_NAME_LABEL, NAME_PROPERTY, databaseName ) );
        return node.flatMap( CommunityTopologyGraphDbmsModel::getTargetedDatabase );
    }

    private Optional<NamedDatabaseId> getDatabaseIdBy( String propertyKey, String propertyValue )
    {
        try
        {
            var node = tx.findNode( DATABASE_LABEL, propertyKey, propertyValue );

            if ( node == null )
            {
                return Optional.empty();
            }

            var databaseName = getPropertyOnNode( DATABASE_LABEL.name(), node, DATABASE_NAME_PROPERTY, String.class );
            var databaseUuid = getPropertyOnNode( DATABASE_LABEL.name(), node, DATABASE_UUID_PROPERTY, String.class );

            return Optional.of( DatabaseIdFactory.from( databaseName, UUID.fromString( databaseUuid ) ) );
        }
        catch ( RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * *Note* may return `Optional.empty`.
     *
     * It s semantically invalid for an alias to *not* have target, but we ignore it because of the possibility of concurrent deletes.
     */
    private static Optional<NamedDatabaseId> getTargetedDatabase( Node aliasNode )
    {
        return ignoreConcurrentDeletes( () ->
        {
            var targetDatabases = StreamSupport.stream( aliasNode.getRelationships( Direction.OUTGOING, TARGETS_RELATIONSHIP ).spliterator(), false )
                                               .collect( Collectors.toList() ); // Must be collected to exhaust the underlying iterator

            return targetDatabases.stream().findFirst()
                                  .map( Relationship::getEndNode )
                                  .map( CommunityTopologyGraphDbmsModel::getDatabaseId );
        } );
    }

    private static Optional<DriverSettings> getDriverSettings( Node aliasNode )
    {
        return ignoreConcurrentDeletes( () ->
        {
            var connectsWith = StreamSupport.stream( aliasNode.getRelationships( Direction.OUTGOING, CONNECTS_WITH_RELATIONSHIP ).spliterator(), false )
                    .collect( Collectors.toList() ); // Must be collected to exhaust the underlying iterator

            return connectsWith.stream().findFirst()
                               .map( Relationship::getEndNode )
                               .map( CommunityTopologyGraphDbmsModel::createDriverSettings );
        } );
    }

    private static Optional<ExternalDatabaseCredentials> getDatabaseCredentials( Node aliasNode )
    {
        return ignoreConcurrentDeletes( () ->
        {
            var username = getPropertyOnNode( REMOTE_DATABASE, aliasNode, USERNAME_PROPERTY, String.class );
            var password = getPropertyOnNode( REMOTE_DATABASE, aliasNode, PASSWORD_PROPERTY,  byte[].class );
            var iv = getPropertyOnNode( REMOTE_DATABASE, aliasNode, IV_PROPERTY,  byte[].class );
            return Optional.of( new ExternalDatabaseCredentials( username, password, iv ) );
        } );
    }

    private static DriverSettings createDriverSettings( Node driverSettingsNode )
    {
        var builder = DriverSettings.builder();
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, SSL_ENABLED.toString(), Boolean.class )
                .map( builder::withSSlEnabled );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, CONNECTION_TIMEOUT.toString(), DurationValue.class )
                .map( builder::withConnectionTimeout );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, CONNECTION_MAX_LIFETIME.toString(), DurationValue.class )
                .map( builder::withConnectionMaxLifeTime );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, CONNECTION_POOL_ACQUISITION_TIMEOUT.toString(), DurationValue.class )
                .map( builder::withConnectionPoolAcquisitionTimeout );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, CONNECTION_POOL_IDLE_TEST.toString(), DurationValue.class )
                .map( builder::withConnectionPoolIdleTest );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, CONNECTION_POOL_MAX_SIZE.toString(), Integer.class )
                .map( builder::withConnectionPoolMaxSize );
        getOptionalPropertyOnNode( DRIVER_SETTINGS, driverSettingsNode, LOGGING_LEVEL.toString(), String.class )
                .map( Level::valueOf )
                .map( builder::withLoggingLevel );

        return builder.build();
    }

    private static NamedDatabaseId getDatabaseId( Node databaseNode )
    {
        var name = (String) databaseNode.getProperty( DATABASE_NAME_PROPERTY );
        var uuid = UUID.fromString( (String) databaseNode.getProperty( DATABASE_UUID_PROPERTY ) );
        return DatabaseIdFactory.from( name, uuid );
    }

    private static <T> Optional<T> getOptionalPropertyOnNode( String labelName, Node node, String key, Class<T> type )
    {
        Object value;
        try
        {
            value = node.getProperty( key );
        }
        catch ( NotFoundException e )
        {
            return Optional.empty();
        }

        if ( value == null )
        {
           return Optional.empty();
        }

        if ( !type.isInstance( value ) )
        {
            throw new IllegalStateException( String.format( "%s has non %s property %s.", labelName, type.getSimpleName(), key ) );
        }

        return Optional.of( type.cast( value ) );
    }

    private static <T> T getPropertyOnNode( String labelName, Node node, String key, Class<T> type )
    {
        var value = node.getProperty( key );
        if ( value == null )
        {
            throw new IllegalStateException( String.format( "%s has no property %s.", labelName, key ) );
        }
        if ( !type.isInstance( value ) )
        {
            throw new IllegalStateException( String.format( "%s has non %s property %s.", labelName, type.getSimpleName(), key ) );
        }
        return type.cast( value );
    }

    private static <T> Optional<T> ignoreConcurrentDeletes( Supplier<Optional<T>> operation )
    {
        try
        {
            return operation.get();
        }
        catch ( NotFoundException e )
        {
            return Optional.empty();
        }
    }
}
