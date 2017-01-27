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
package org.neo4j.server.security.enterprise.configuration;


import org.junit.Test;

import java.util.Collections;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SecurityConfigurationValidatorTest
{


    @Test
    public void shouldWarnIfUsingSeveralConfigs() throws Throwable
    {
        try
        {
            Config.embeddedDefaults( (stringMap( SecuritySettings.auth_providers.name(), "native, LDAP",
                    SecuritySettings.auth_provider.name(), "native" )),
                    Collections.singleton( new SecurityConfigurationValidator() ) );

            fail();
        }
        catch ( InvalidSettingException invalid )
        {
            assertEquals( "Using both auth_provider and auth_providers and they do not match:" +
                    " auth_provider = native , auth_provider = [native, LDAP]", invalid.getMessage() );
        }
    }

    @Test
    public void shouldWarnIfUsingConflictingConfigs() throws Throwable
    {
        try
        {
            Config.embeddedDefaults(
                    (stringMap( SecuritySettings.auth_providers.name(), "LDAP", SecuritySettings.auth_provider.name(),
                            "native" )), Collections.singleton( new SecurityConfigurationValidator() ) );
            fail();
        }
        catch ( InvalidSettingException invalid )
        {
            assertEquals( "Using both auth_provider and auth_providers and they do not match:" +
                    " auth_provider = native , auth_provider = [LDAP]", invalid.getMessage() );
        }
    }

    @Test
    public void shouldNotWarnIfUsingAgreeingConfigs() throws Throwable
    {
        Config.embeddedDefaults(
                (stringMap( SecuritySettings.auth_providers.name(), "native", SecuritySettings.auth_provider.name(),
                        "native" )), Collections.singleton( new SecurityConfigurationValidator() ) );

    }
}
