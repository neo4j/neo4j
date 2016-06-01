/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Test configuration migration rules
 */
public class TestGraphDatabaseConfigurationMigrator
{
    @Test
    public void testNoMigration()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();
        assertThat( migrator.apply( stringMap( "foo", "bar" ), NullLog.getInstance() ), equalTo( stringMap( "foo", "bar" ) ) );
    }

    @Test
    public void migrateIndexSamplingBufferSizeIfPresent()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.buffer_size", "64m" ), NullLog.getInstance() );
        assertEquals( "Old property should be migrated to new one with correct value",
                resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388608" ));
    }

    @Test
    public void skipMigrationOfIndexSamplingBufferSizeIfNotPresent()
    {
        ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ), NullLog.getInstance() );
        assertEquals( "Nothing to migrate should be the same",
                resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ));
    }
}
