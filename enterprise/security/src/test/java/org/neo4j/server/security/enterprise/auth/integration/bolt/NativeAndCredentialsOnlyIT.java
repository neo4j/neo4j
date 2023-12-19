/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

public class NativeAndCredentialsOnlyIT extends EnterpriseAuthenticationTestBase
{
    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction()
                .andThen( settings -> settings.put( SecuritySettings.auth_providers, "native,plugin-TestCredentialsOnlyPlugin" ) );
    }

    @Test
    public void shouldAuthenticateWithCredentialsOnlyPlugin() throws Throwable
    {
        assertAuth( "", "BASE64-ENC-PASSWORD", "plugin-TestCredentialsOnlyPlugin" );
    }
}
