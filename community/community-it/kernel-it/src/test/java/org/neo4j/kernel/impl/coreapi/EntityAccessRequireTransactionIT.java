/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.coreapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DbmsExtension
public class EntityAccessRequireTransactionIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;
    private InternalTransaction transaction;

    static Stream<EntityFactory<? extends Entity>> entities()
    {
        return Stream.of( tx -> new NodeEntity( tx, 1 ), tx -> new RelationshipEntity( tx, 1 ) );
    }

    @BeforeEach
    void setUp()
    {
        transaction = (InternalTransaction) databaseAPI.beginTx();
    }

    @AfterEach
    void tearDown()
    {
        if ( transaction != null )
        {
            transaction.close();
        }
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void entityIdAccessWithoutTransaction( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertDoesNotThrow( entity::getId );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessAllProperties( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, entity::getAllProperties );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessCheckPropertyExistence( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.hasProperty( "any" ) );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessGetProperty( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.getProperty( "any" ) );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessGetPropertyWithDefaultValue( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.getProperty( "any", "foo" ) );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessSetProperty( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.setProperty( "any", "bar" ) );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessRemoveProperty( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.removeProperty( "any" ) );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessAllPropertyKeys( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, entity::getPropertyKeys );
    }

    @ParameterizedTest
    @MethodSource( "entities" )
    void requireTransactionToAccessMultipleProperties( EntityFactory<Entity> entityFactory )
    {
        var entity = getDetachedEntity( entityFactory );
        assertThrows( NotInTransactionException.class, () -> entity.getProperties( "a", "b", "c" ) );
    }

    private Entity getDetachedEntity( EntityFactory<Entity> entityFactory )
    {
        var entity = entityFactory.apply( transaction );
        transaction.close();
        return entity;
    }

    @FunctionalInterface
    private interface EntityFactory<T> extends Function<InternalTransaction,Entity>
    {

    }
}
