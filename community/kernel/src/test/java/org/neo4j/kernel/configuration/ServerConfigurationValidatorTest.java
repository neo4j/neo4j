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
package org.neo4j.kernel.configuration;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.config.InvalidSettingException;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

@EnableRuleMigrationSupport
public class ServerConfigurationValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void httpConnectorIsRequired()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory enabled connector of type 'HTTP'" );

        // when
        Config.fromSettings(
                stringMap( "dbms.connector.http.enabled", "false",
                        "dbms.connector.https.enabled", "false") ).withServerDefaults().build();
    }
}
