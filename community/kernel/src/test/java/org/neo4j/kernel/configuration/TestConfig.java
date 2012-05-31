/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.Migrator;

public class TestConfig {

	public static class MyMigratingSettings
	{
		@Migrator
		public static ConfigurationMigrator migrator = new BaseConfigurationMigrator(){
			{
				add(new SpecificPropertyMigration("old", "Old has been replaced by newer!")
				{
					@Override
					public void setValueWithOldSetting(String value,
							Map<String, String> rawConfiguration) {
						rawConfiguration.put(newer.name(), value);
					}
				});
			}
		};
		
		public static GraphDatabaseSetting.StringSetting newer = new GraphDatabaseSetting.StringSetting("hello", ".*", "");
	}
	
	public static class MySettingsWithDefaults
	{
		@Default("Hello, World!")
		public static GraphDatabaseSetting.StringSetting hello = new GraphDatabaseSetting.StringSetting("hello", ".*", "");
		
	}
	
	@Test
	public void shouldApplyDefaults()
	{
		Config config = new Config(new HashMap<String,String>(), MySettingsWithDefaults.class);
		
		assertThat(config.get(MySettingsWithDefaults.hello), is("Hello, World!"));
	}
	
	@Test
	public void shouldApplyMigrations()
	{
		
		Map<String, String> params = new HashMap<String,String>();
		params.put("old", "hello!");
		
		Config config = new Config(params, MyMigratingSettings.class);
		assertThat(config.get(MyMigratingSettings.newer), is("hello!"));
	}
	
}
