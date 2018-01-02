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

import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.AssertableLogProvider.inLog;

/**
 * Test configuration migration rules
 */
public class TestGraphDatabaseConfigurationMigrator
{
    @Test
    public void testNoMigration()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "foo", "bar" ), NullLog.getInstance() ), equalTo( stringMap( "foo", "bar" ) ) );
    }

    @Test
    public void testEnableOnlineBackup()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "enable_online_backup", "true" ), NullLog.getInstance()  ),
                equalTo( stringMap( "online_backup_enabled", "true", "online_backup_server", "0.0.0.0:6362-6372" ) ) );

        // 1.9
        assertThat( migrator.apply( stringMap( "online_backup_port", "1234" ), NullLog.getInstance()  ),
                equalTo( stringMap( "online_backup_server", "0.0.0.0:1234" ) ) );
    }

    @Test
    public void testUdcEnabled()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "true" ), NullLog.getInstance()  ),
                equalTo( stringMap( "neo4j.ext.udc.enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "neo4j.ext.udc.disable", "false" ), NullLog.getInstance()  ),
                equalTo( stringMap( "neo4j.ext.udc.enabled", "true" ) ) );
    }

    @Test
    public void testEnableRemoteShell()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "true" ), NullLog.getInstance()  ),
                equalTo( stringMap( "remote_shell_enabled", "true" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "false" ), NullLog.getInstance()  ),
                equalTo( stringMap( "remote_shell_enabled", "false" ) ) );
        assertThat( migrator.apply( stringMap( "enable_remote_shell", "port=1234" ), NullLog.getInstance()  ),
                equalTo( stringMap( "remote_shell_enabled", "true","remote_shell_port","1234","remote_shell_read_only","false","remote_shell_name","shell" ) ) );
    }

    @Test
    public void testMemoryMappingIsTotalConfiguredForAllStores() throws Exception
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator(  );
        AssertableLogProvider logProvider = new AssertableLogProvider();

        Map<String, String> oldConfig = stringMap(
                "neostore.nodestore.db.mapped_memory", "12M",
                "neostore.propertystore.db.mapped_memory", "1G",
                "neostore.propertystore.db.index.mapped_memory", "1M",
                "neostore.propertystore.db.index.keys.mapped_memory", "13",
                "neostore.propertystore.db.strings.mapped_memory", "2",
                "neostore.propertystore.db.arrays.mapped_memory", "1",
                "neostore.relationshipstore.db.mapped_memory", "0" );

        // When & Then
        assertThat( migrator.apply( oldConfig, logProvider.getLog( getClass() ) ).get( pagecache_memory.name() ),
            equalTo( "1074790416" ) );

        logProvider.assertAtLeastOnce(
                inLog( getClass() )
                        .warn( "The neostore.*.db.mapped_memory settings have been replaced by the single 'dbms" +
                               ".pagecache.memory'. The sum of the old configuration will be used as the value for the new setting." )
        );
    }

    @Test
    public void shouldWarnAboutDeprecatedCacheTypeSetting() throws Exception
    {
        // given
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Map<String,String> oldConfig = stringMap( GraphDatabaseSettings.cache_type.name(), "hpc" );

        // when
        Map<String,String> newConfig = migrator.apply( oldConfig, logProvider.getLog( getClass() ) );

        // then
        logProvider.assertAtLeastOnce(
                inLog( getClass() ).warn( CoreMatchers.containsString( "cache_type" ) )
        );
        assertFalse( newConfig.containsKey( "cache_type" ) );
    }

    @Test
    public void shouldNotWarnAboutDeprecatedCacheTypeSettingWithDefaultValue() throws Exception
    {
        // given
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Map<String,String> oldConfig = stringMap(
                GraphDatabaseSettings.cache_type.name(), GraphDatabaseSettings.cache_type.getDefaultValue() );

        // when
        Map<String,String> newConfig = migrator.apply( oldConfig, logProvider.getLog( getClass() ) );

        // then
        logProvider.assertNoLoggingOccurred();
        assertFalse( newConfig.containsKey( "cache_type" ) );
    }
}
