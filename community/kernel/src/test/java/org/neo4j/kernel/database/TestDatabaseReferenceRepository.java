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
package org.neo4j.kernel.database;

import org.apache.commons.lang3.RandomStringUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

public final class TestDatabaseReferenceRepository
{

    public static DatabaseReference randomAnyDatabaseReference()
    {
        var databaseName = RandomStringUtils.randomAlphabetic( 10 );
        return anyDatabaseReference( databaseName );
    }

    public static DatabaseReference anyDatabaseReference( String databaseName )
    {
        var internal = ThreadLocalRandom.current().nextBoolean();
        return internal ? internalDatabaseReference( databaseName ) :
               externalDatabaseReference( databaseName );
    }

    public static DatabaseReference.Internal randomInternalDatabaseReference()
    {
        var databaseName = RandomStringUtils.randomAlphabetic( 10 );
        return internalDatabaseReference( databaseName );
    }

    public static DatabaseReference.External randomExternalDatabaseReference()
    {
        var databaseName = RandomStringUtils.randomAlphabetic( 10 );
        return externalDatabaseReference( databaseName );
    }

    public static DatabaseReference.Internal internalDatabaseReference( String databaseName )
    {
        return internalDatabaseReference( databaseName, databaseName );
    }

    public static DatabaseReference.Internal internalDatabaseReference( String databaseName, String aliasName )
    {
        var normalizedAlias = new NormalizedDatabaseName( aliasName );
        var dbId = DatabaseIdFactory.from( databaseName, UUID.nameUUIDFromBytes( databaseName.getBytes( UTF_8 ) ) );
        return new DatabaseReference.Internal( normalizedAlias, dbId );
    }

    public static DatabaseReference.External externalDatabaseReference( String databaseName )
    {
        return externalDatabaseReference( databaseName, databaseName );
    }

    public static DatabaseReference.External externalDatabaseReference( String localAliasName, String targetDatabaseName )
    {
        var normalizedAlias = new NormalizedDatabaseName( localAliasName );
        var normalizedTarget = new NormalizedDatabaseName( targetDatabaseName );
        var addr = SocketAddressParser.socketAddress( URI.create( "my.neo4j.com" ), BoltConnector.DEFAULT_PORT, SocketAddress::new );
        var uri = new RemoteUri( "neo4j", List.of( addr ), null );
        var uuid = UUID.randomUUID();
        return new DatabaseReference.External( normalizedTarget, normalizedAlias, uri, uuid );
    }

    public static class Fixed implements DatabaseReferenceRepository
    {
        private static final DatabaseReference SYSTEM_DATABASE_REFERENCE =
                new DatabaseReference.Internal( new NormalizedDatabaseName( SYSTEM_DATABASE_NAME ), NAMED_SYSTEM_DATABASE_ID );

        private final Map<NormalizedDatabaseName,DatabaseReference> databaseReferences;

        public Fixed( Collection<DatabaseReference> databaseReferences )
        {
            this.databaseReferences = databaseReferences.stream()
                                                        .collect( Collectors.toMap( DatabaseReference::alias, identity() ) );
        }

        public Fixed( DatabaseReference... databaseReferences )
        {
            this.databaseReferences = Arrays.stream( databaseReferences )
                                            .collect( Collectors.toMap( DatabaseReference::alias, identity() ) );
        }

        @Override
        public Optional<DatabaseReference> getByName( NormalizedDatabaseName databaseName )
        {
            if ( Objects.equals( SYSTEM_DATABASE_NAME, databaseName.name() ) )
            {
                return Optional.of( SYSTEM_DATABASE_REFERENCE );
            }
            return Optional.ofNullable( databaseReferences.get( databaseName ) );
        }

        @Override
        public Set<DatabaseReference> getAllDatabaseReferences()
        {
            return Set.copyOf( databaseReferences.values() );
        }

        @Override
        public Set<DatabaseReference.Internal> getInternalDatabaseReferences()
        {
            return getDatabaseReferences( DatabaseReference.Internal.class );
        }

        @Override
        public Set<DatabaseReference.External> getExternalDatabaseReferences()
        {
            return getDatabaseReferences( DatabaseReference.External.class );
        }

        private <T extends DatabaseReference> Set<T> getDatabaseReferences( Class<T> type )
        {
            return databaseReferences.values().stream()
                                     .filter( type::isInstance )
                                     .map( type::cast )
                                     .collect( Collectors.toSet() );
        }

        public void setDatabaseReference( NormalizedDatabaseName databaseName, DatabaseReference databaseRef )
        {
            databaseReferences.put( databaseName, databaseRef );
        }

        public void removeDatabaseReference( NormalizedDatabaseName databaseName )
        {
            databaseReferences.remove( databaseName );
        }
    }
}
