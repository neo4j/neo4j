/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.domain;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.collection.MapUtil.map;

public class PropertySettingStrategyTest
{
    private static GraphDatabaseAPI db;
    private Transaction tx;
    private static PropertySettingStrategy propSetter;

    @BeforeAll
    public static void createDb()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        propSetter = new PropertySettingStrategy( db );
    }

    @AfterAll
    public static void closeDb()
    {
        db.shutdown();
    }

    @BeforeEach
    public void beginTx()
    {
        tx = db.beginTx();
    }

    @AfterEach
    public void rollbackTx()
    {
        tx.close();
    }

    @Test
    public void shouldSetSingleProperty() throws Exception
    {
        // Given
        Node node = db.createNode();

        // When
        propSetter.setProperty( node, "name", "bob" );

        // Then
        assertThat( node.getProperty( "name" ), is("bob"));
    }

    @Test
    public void shouldSetMultipleProperties() throws Exception
    {
        // Given
        Node node = db.createNode();

        List<String> anArray = new ArrayList<>();
        anArray.add( "hello" );
        anArray.add( "Iamanarray" );

        Map<String, Object> props = new HashMap<>();
        props.put( "name", "bob" );
        props.put( "age", 12 );
        props.put( "anArray", anArray );

        // When
        propSetter.setProperties( node, props );

        // Then
        assertThat( node.getProperty( "name" ), is("bob"));
        assertThat( node.getProperty( "age" ), is(12));
        assertThat( node.getProperty( "anArray" ), is(new String[]{"hello","Iamanarray"}));
    }

    @Test
    public void shouldSetAllProperties() throws Exception
    {
        // Given
        Node node = db.createNode();
        node.setProperty( "name", "bob" );
        node.setProperty( "age", 12 );

        // When
        propSetter.setAllProperties( node, map( "name", "Steven", "color", 123 ) );

        // Then
        assertThat( node.getProperty( "name" ), is("Steven"));
        assertThat( node.getProperty( "color" ), is(123));
        assertThat( node.hasProperty( "age" ), is(false));
    }

    // Handling empty collections

    @Test
    public void shouldNotFailSettingEmptyArrayIfEntityAlreadyHasAnEmptyArrayAsValue() throws Exception
    {
        // Given
        Node node = db.createNode();
        node.setProperty( "arr", new String[]{} );

        // When
        propSetter.setProperty( node, "arr", new ArrayList<>() );

        // Then
        assertThat( node.getProperty( "arr" ), is(new String[]{}));
    }

    @Test
    public void shouldNotFailSettingEmptyArrayAndOtherValuesIfEntityAlreadyHasAnEmptyArrayAsValue() throws Exception
    {
        // Given
        Node node = db.createNode();
        node.setProperty( "arr", new String[]{} );

        Map<String, Object> props = new HashMap<>();
        props.put( "name", "bob" );
        props.put( "arr", new ArrayList<String>(  ) );

        // When
        propSetter.setProperties( node, props );

        // Then
        assertThat( node.getProperty( "name" ), is("bob"));
        assertThat( node.getProperty( "arr" ), is(new String[]{}));
    }

    @Test
    public void shouldThrowPropertyErrorWhenSettingEmptyArrayOnEntityWithNoPreExistingProperty()
    {
        assertThrows( PropertyValueException.class, () -> {
            // Given
            Node node = db.createNode();

            // When
            propSetter.setProperty( node, "arr", new ArrayList<>() );
        } );
    }

    @Test
    public void shouldThrowPropertyErrorWhenSettingEmptyArrayOnEntityWithNoPreExistingEmptyArray()
    {
        assertThrows( PropertyValueException.class, () -> {
            // Given
            Node node = db.createNode();
            node.setProperty( "arr", "hello" );

            // When
            propSetter.setProperty( node, "arr", new ArrayList<>() );
        } );
    }

    @Test
    public void shouldUseOriginalTypeWhenSettingEmptyArrayIfEntityAlreadyHasACollection() throws Exception
    {
        // Given
        Node node = db.createNode();
        node.setProperty( "arr", new String[]{"a","b"} );

        // When
        propSetter.setProperty( node, "arr", new ArrayList<>() );

        // Then
        assertThat( node.getProperty( "arr" ), is(new String[]{}));
    }

    @Test
    public void shouldUseOriginalTypeOnEmptyCollectionWhenSettingAllProperties() throws Exception
    {
        // Given
        Node node = db.createNode();
        node.setProperty( "name", "bob" );
        node.setProperty( "arr", new String[]{"a","b"} );

        // When
        propSetter.setAllProperties( node, map("arr", new ArrayList<String>()) );

        // Then
        assertThat( node.hasProperty( "name" ), is(false));
        assertThat( node.getProperty( "arr" ), is(new String[]{}));
    }

}
