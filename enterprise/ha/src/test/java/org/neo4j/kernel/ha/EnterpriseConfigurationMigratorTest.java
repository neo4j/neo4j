/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.hamcrest.CoreMatchers.is;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * EnterpriseConfigurationMigrator tests
 */
public class EnterpriseConfigurationMigratorTest
{
    EnterpriseConfigurationMigrator migrator = new EnterpriseConfigurationMigrator();
    
    @Test
    public void testOnlineBackupMigration()
        throws Exception
    {
        Map<String, String> original = MapUtil.stringMap( "enable_online_backup", "true" );
        Map<String, String> migrated = migrator.apply( original, StringLogger.DEV_NULL );
        Assert.assertThat( migrated.containsKey( "enable_online_backup" ), is( false ) );
        Assert.assertThat( migrated.get( "online_backup_enabled" ), is( "true" ) );
        Assert.assertThat( migrated.get( "online_backup_server" ), is( OnlineBackupSettings.online_backup_server.getDefaultValue() ) );
    }

    @Test
    public void testOnlineBackupPortMigration()
        throws Exception
    {
        Map<String, String> original = MapUtil.stringMap( "enable_online_backup", "port=123" );
        Map<String, String> migrated = migrator.apply( original, StringLogger.DEV_NULL );
        Assert.assertThat( migrated.containsKey( "enable_online_backup" ), is( false ) );
        Assert.assertThat( migrated.get( "online_backup_enabled" ), is( "true" ) );
        Assert.assertThat( migrated.get( "online_backup_server" ), is( "0.0.0.0:123" ) );
    }

    @Test
    public void testMachineIdMigration()
        throws Exception
    {
        Map<String, String> original = MapUtil.stringMap( "ha.machine_id", "123" );
        Map<String, String> migrated = migrator.apply( original, StringLogger.DEV_NULL );
        Assert.assertThat( migrated.containsKey( "ha.machine_id" ), is( false ) );
        Assert.assertThat( migrated.get( "ha.server_id" ), is( "123" ) );
    }
}
