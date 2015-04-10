/**
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
package org.neo4j.mjolnir.launcher;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.Settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ConfigOptionsTest
{
    public static class MySettings
    {
        @Description( "Some setting" )
        public static final Setting<String> mySetting = Settings.setting("the.key", Settings.STRING, "hello" );
    }

    @Test
    public void shouldDescribeConfigOptions() throws Throwable
    {
        // Given
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // When
        new ConfigOptions(MySettings.class).describeTo( new PrintStream( baos ) );

        // Then
        assertThat(baos.toString( "UTF-8" ), equalTo(
            "the.key  Some setting [default: hello]\n"));
    }
}