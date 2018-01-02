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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class GraphDatabaseSettingsTest
{
    @Test
    public void mustHaveReasonableDefaultPageCacheMemorySizeInBytes() throws Exception
    {
        long bytes = new Config().get( GraphDatabaseSettings.pagecache_memory );
        assertThat( bytes, greaterThanOrEqualTo( ByteUnit.mebiBytes( 32 ) ) );
        assertThat( bytes, lessThanOrEqualTo( ByteUnit.tebiBytes( 1 ) ) );
    }

    @Test
    public void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue() throws Exception
    {
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        assertThat( new Config( stringMap( name, "245760" ) ).get( setting ), is( ByteUnit.kibiBytes( 240 ) ) );
        assertThat( new Config( stringMap( name, "2244g" ) ).get( setting ), is( ByteUnit.gibiBytes( 2244 ) ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void pageCacheSettingMustRejectOverlyConstrainedMemorySetting() throws Exception
    {
        long pageSize = new Config().get( GraphDatabaseSettings.mapped_memory_page_size );
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        // We configure the page cache to have one byte less than two pages worth of memory. This must throw:
        new Config( stringMap( name, "" + (pageSize * 2 - 1) ) ).get( setting );
    }

    @Test
    public void noDuplicateSettingsAreAllowed() throws Exception
    {
        final HashMap<String,String> fields = new HashMap<>();
        for ( Field field : GraphDatabaseSettings.class.getDeclaredFields() )
        {
            if ( field.getType() == Setting.class )
            {
                Setting setting = (Setting) field.get( null );

                assertFalse(
                        String.format( "'%s' in %s has already been defined in %s", setting.name(), field.getName(),
                                fields.get( setting.name() ) ), fields.containsKey( setting.name() ) );
                fields.put( setting.name(), field.getName() );
            }
        }
    }
}
