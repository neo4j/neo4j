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
package org.neo4j.server.rest.web;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.server.configuration.ServerSettings;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ScriptExecutionModeTest
{
    @Test
    public void unrestrictedConfiguration() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap(
                ServerSettings.script_enabled.name(), Settings.TRUE,
                ServerSettings.script_sandboxing_enabled.name(), Settings.FALSE ) );
        assertThat( ScriptExecutionMode.getConfiguredMode( config ), is( ScriptExecutionMode.UNRESTRICTED ) );
    }

    @Test
    public void sandboxedConfiguration() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap(
                ServerSettings.script_enabled.name(), Settings.TRUE,
                ServerSettings.script_sandboxing_enabled.name(), Settings.TRUE ) );
        assertThat( ScriptExecutionMode.getConfiguredMode( config ), is( ScriptExecutionMode.SANDBOXED ) );
    }

    @Test
    public void disabledConfiguration() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap(
                ServerSettings.script_enabled.name(), Settings.FALSE,
                ServerSettings.script_sandboxing_enabled.name(), Settings.TRUE ) );
        assertThat( ScriptExecutionMode.getConfiguredMode( config ), is( ScriptExecutionMode.DISABLED ) );

        config = Config.defaults();
        config.augment( stringMap(
                ServerSettings.script_enabled.name(), Settings.FALSE,
                ServerSettings.script_sandboxing_enabled.name(), Settings.FALSE ) );
        assertThat( ScriptExecutionMode.getConfiguredMode( config ), is( ScriptExecutionMode.DISABLED ) );

        config = Config.defaults();
        config.augment( stringMap(
                ServerSettings.script_enabled.name(), Settings.FALSE ) );
        assertThat( ScriptExecutionMode.getConfiguredMode( config ), is( ScriptExecutionMode.DISABLED ) );
    }
}
