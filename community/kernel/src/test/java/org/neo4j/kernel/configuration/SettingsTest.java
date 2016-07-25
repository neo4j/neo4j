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

import java.io.File;

import org.junit.Test;

import org.neo4j.graphdb.config.Setting;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.pathSetting;

public class SettingsTest
{
    @Test
    public void parsesAbsolutePaths()
    {
        File absolutePath = new File( "some/path" ).getAbsoluteFile();
        File thePath = Settings.PATH.apply( absolutePath.toString() );

        assertEquals( absolutePath, thePath );
    }

    @Test
    public void doesntAllowRelativePaths()
    {
        File relativePath = new File( "some/path" );
        try
        {
            Settings.PATH.apply( relativePath.toString() );
            fail( "Expected an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void pathSettingsProvideDefaultValues()
    {
        File theDefault = new File( "/some/path" ).getAbsoluteFile();
        Setting<File> setting = pathSetting( "some.setting", theDefault.getAbsolutePath() );
        assertThat( setting.from( Config.empty() ), is( theDefault ) );
    }

    @Test
    public void pathSettingsAreNullIfThereIsNoValueAndNoDefault()
    {
        Setting<File> setting = pathSetting( "some.setting", NO_DEFAULT );
        assertThat( setting.from( Config.empty() ), is( nullValue() ) );
    }

    @Test
    public void shouldHaveAUsefulToStringWhichIsUsedAsTheValidValuesInDocumentation()
    {
        assertThat( pathSetting( "", NO_DEFAULT ).toString(), containsString( "A filesystem path" ) );
    }
}
