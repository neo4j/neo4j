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

import java.util.Map;

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
    public void shouldNotAllowPublisherCreateNewTokens() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        assertFail( writeSubject, "CREATE (:MySpecialLabel)", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "MATCH (a:Node), (b:Node) WHERE a.number = 0 AND b.number = 1 " +
                "CREATE (a)-[:MySpecialRelationship]->(b)", TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( writeSubject, "CREATE (a) SET a.MySpecialProperty = 'a'", TOKEN_CREATE_OPS_NOT_ALLOWED );
    }

    @Test
    public void shouldAllowPublisherCreateNewTokensWhenConfigured() throws Throwable
    {
        Map<String,String> settingsMap = defaultConfiguration();
        settingsMap.put( SecuritySettings.allow_publisher_create_token.name(), "true" );
        configuredSetup( stringMap( settingsMap ) );
        assertEmpty( writeSubject, "CREATE (:MySpecialLabel)" );
        assertEmpty( writeSubject, "MATCH (a:Node), (b:Node) WHERE a.number = 0 AND b.number = 1 " +
                "CREATE (a)-[:MySpecialRelationship]->(b)" );
        assertEmpty( writeSubject, "CREATE (a) SET a.MySpecialProperty = 'a'" );
    }
}
