/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public abstract class ConfiguredAuthScenariosInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{
    @Override
    public void setUp() throws Throwable
    {
        // tests are required to setup database with specific configs
    }

    @Test
    public void shouldAllowPublisherCreateNewTokens() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        assertEmpty( writeSubject, "CREATE (:MySpecialLabel)" );
        assertEmpty( writeSubject, "MATCH (a:Node), (b:Node) WHERE a.number = 0 AND b.number = 1 " +
                "CREATE (a)-[:MySpecialRelationship]->(b)" );
        assertEmpty( writeSubject, "CREATE (a) SET a.MySpecialProperty = 'a'" );
    }

    @Test
    public void shouldNotAllowPublisherCreateNewTokensWhenConfigured() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.allow_publisher_create_token.name(), "false" ) );
        assertFail( writeSubject, "CREATE (:MySpecialLabel)", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "MATCH (a:Node), (b:Node) WHERE a.number = 0 AND b.number = 1 " +
                "CREATE (a)-[:MySpecialRelationship]->(b)", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "CREATE (a) SET a.MySpecialProperty = 'a'", TOKEN_CREATE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldAllowPublisherCallCreateNewTokensProcedures() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        assertEmpty( writeSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertEmpty( writeSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertEmpty( writeSubject, "CALL db.createProperty('MySpecialProperty')" );
    }

    @Test
    public void shouldNotAllowPublisherCallCreateNewTokensProceduresWhenConfigured() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.allow_publisher_create_token.name(), "false" ) );
        assertFail( writeSubject, "CALL db.createLabel('MySpecialLabel')", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "CALL db.createRelationshipType('MySpecialRelationship')", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "CALL db.createProperty('MySpecialProperty')", TOKEN_CREATE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldAllowRoleCallCreateNewTokensProceduresWhenConfigured() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );
        userManager.newRole( "role1", "noneSubject" );
        assertEmpty( noneSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertEmpty( noneSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertEmpty( noneSubject, "CALL db.createProperty('MySpecialProperty')" );
    }
}
