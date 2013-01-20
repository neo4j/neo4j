/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.*;

/**
 * Test configuration migration rules
 */
public class TestGraphDatabaseConfigurationMigrator
{
    @Test
    public void testNoMigration()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "foo", "bar" ), StringLogger.DEV_NULL ), equalTo( stringMap( "foo", "bar" ) ) );
    }

    @Test
    public void testEnableOnlineBackup()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "enable_online_backup", "true" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "online_backup_enabled", "true", "online_backup_server", ":6372-6382" ) ) );

        // 1.9
        assertThat( migrator.apply( stringMap( "online_backup_port", "1234" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "online_backup_server", ":1234" ) ) );
    }

    @Test
    public void testUdcEnabled()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "true" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "neo4j.ext.udc.enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "false" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "neo4j.ext.udc.enabled", "true" ) ) );
    }

    @Test
    public void testEnableRemoteShell()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "true" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "remote_shell_enabled", "true" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "false" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "remote_shell_enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "port=1234" ), StringLogger.DEV_NULL  ), equalTo( stringMap( "remote_shell_enabled", "true","remote_shell_port","1234","remote_shell_read_only","false","remote_shell_name","shell" ) ) );
    }
}
