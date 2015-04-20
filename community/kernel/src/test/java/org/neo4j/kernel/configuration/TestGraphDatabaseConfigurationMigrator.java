/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.warn;

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
        assertThat( migrator.apply( stringMap( "enable_online_backup", "true" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "online_backup_enabled", "true", "online_backup_server", "0.0.0.0:6362-6372" ) ) );

        // 1.9
        assertThat( migrator.apply( stringMap( "online_backup_port", "1234" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "online_backup_server", "0.0.0.0:1234" ) ) );
    }

    @Test
    public void testUdcEnabled()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "true" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "neo4j.ext.udc.enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "false" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "neo4j.ext.udc.enabled", "true" ) ) );
    }

    @Test
    public void testEnableRemoteShell()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "true" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "remote_shell_enabled", "true" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "false" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "remote_shell_enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "port=1234" ), StringLogger.DEV_NULL  ),
                equalTo( stringMap( "remote_shell_enabled", "true","remote_shell_port","1234","remote_shell_read_only","false","remote_shell_name","shell" ) ) );
    }

    @Test
    public void testGCRRenamedToHPC()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        TestLogger log = new TestLogger();

        // When & Then
        assertThat( migrator.apply( stringMap( "cache_type", "gcr" ), log ),
                equalTo( stringMap( "cache_type", "hpc" ) ) );

        log.assertAtLeastOnce( warn( "'gcr' cache type has been renamed to 'hpc', High Performance Cache." ) );
    }

    @Test
    public void testMemoryMappingIsTotalConfiguredForAllStores() throws Exception
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        TestLogger log = new TestLogger();

        Map<String, String> oldConfig = stringMap(
                "neostore.nodestore.db.mapped_memory", "12M",
                "neostore.propertystore.db.mapped_memory", "1G",
                "neostore.propertystore.db.index.mapped_memory", "1M",
                "neostore.propertystore.db.index.keys.mapped_memory", "13",
                "neostore.propertystore.db.strings.mapped_memory", "2",
                "neostore.propertystore.db.arrays.mapped_memory", "1",
                "neostore.relationshipstore.db.mapped_memory", "0" );

        // When & Then
        assertThat( migrator.apply( oldConfig, log ).get( pagecache_memory.name() ),
            equalTo( "1074790416" ) );

        log.assertAtLeastOnce( warn( "The neostore.*.db.mapped_memory settings have been replaced by the single 'dbms.pagecache.memory'. The sum of the old configuration will be used as the value for the new setting." ) );
    }
}
