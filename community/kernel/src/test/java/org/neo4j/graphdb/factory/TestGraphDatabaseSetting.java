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

package org.neo4j.graphdb.factory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.kernel.configuration.Config;

public class TestGraphDatabaseSetting
{
    @Test
    public void testStringSetting()
    {
        GraphDatabaseSetting.StringSetting stringSetting = new GraphDatabaseSetting.StringSetting( "foo_bar", GraphDatabaseSetting.ANY, "Must be a valid foo bar" );
        
        assertThat( stringSetting.name(), equalTo( "foo_bar" ) );

        stringSetting.validate( "test" );

        try
        {
            stringSetting.validate( null );
            fail( "null should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
        }

        try
        {
            stringSetting.validate( "" );
            fail( "empty string should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
        }
    }

    @Test
    public void testIntegerSetting()
    {
        GraphDatabaseSetting.IntegerSetting integerSetting = new GraphDatabaseSetting.IntegerSetting( "foo_bar", "Must be a valid integer", 3, 10 );

        assertThat( integerSetting.name(), equalTo( "foo_bar" ) );

        integerSetting.validate( "5" );

        integerSetting.validate( "3" );

        integerSetting.validate( "10" );

        try
        {
            integerSetting.validate( "2" );
            fail( "too low number should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
            assertThat( e.getMessage(), equalTo( "Invalid value '2' for config property 'foo_bar': Minimum allowed value is: 3" ) );
        }
        
        try
        {
            integerSetting.validate( "11" );
            fail( "too high number should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
        }
    }

    @Test
    public void testOptionsSetting()
    {
        GraphDatabaseSetting.OptionsSetting optionsSetting = new GraphDatabaseSetting.OptionsSetting( "foo_bar", "option1", "option2", "option3" );

        assertThat( optionsSetting.name(), equalTo( "foo_bar" ) );

        optionsSetting.validate( "option1" );

        optionsSetting.validate( "option2" );

        try
        {
            optionsSetting.validate( "option4" );
            fail( "invalid option should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
            assertThat( e.getMessage(), equalTo( "Invalid value 'option4' for config property 'foo_bar': Invalid option. Valid options are:[option1, option2, option3]" ) );
        }
        
    }
    
    @Test
    public void testFileSetting() 
    {
        GraphDatabaseSetting.FileSetting fileSetting = new GraphDatabaseSetting.FileSetting("myfile");
        assertThat( fileSetting.name(), equalTo( "myfile" ) );
        
        fileSetting.validate("/some/path");
        
        try
        {
            fileSetting.validate( null );
            fail( "null paths should not be allowed" );
        }
        catch( IllegalArgumentException e )
        {
            // Ok
            assertThat( e.getMessage(), equalTo( "Invalid value [null] for config property 'myfile': Must be a valid file path." ) );
        }
    }

    @Test
    public void testRelativeFileSetting()
        throws IOException
    {
        GraphDatabaseSetting.DirectorySetting baseDir = new GraphDatabaseSetting.DirectorySetting("myDirectory");
        GraphDatabaseSetting.FileSetting fileSetting = new GraphDatabaseSetting.FileSetting("myfile", baseDir, true, true);
        
        Config config = new Config(new HashMap<String,String>(){{put("myDirectory","/home/jake");}});
        
        // Relative paths
        assertThat(fileSetting.valueOf("baa", config), equalTo(new File("/home/jake/baa").getCanonicalPath()));
        
        // Absolute paths
        if (GraphDatabaseSetting.osIsWindows())
        {
            assertThat(fileSetting.valueOf("c:\\baa", config), equalTo(new File("c:\\baa").getCanonicalPath()));
        }
        else
        {
            assertThat(fileSetting.valueOf("/baa", config), equalTo(new File("/baa").getAbsolutePath()));

            // Path with incorrect directory separator
            assertThat(fileSetting.valueOf("\\baa\\boo", config), equalTo(new File("/baa/boo").getCanonicalPath()));
        }

    }

    @Test
    public void testURISetting() 
    {
        GraphDatabaseSetting.URISetting setting = new GraphDatabaseSetting.URISetting("myfile", true);
        
        Config config = mock(Config.class);
        
        assertThat(setting.valueOf("/baa/boo", config).toString(), equalTo("/baa/boo"));
        
        // Strip trailing slash
        assertThat(setting.valueOf("/baa/", config).toString(), equalTo("/baa"));
        
    }

    @Test
    public void testNumberOfBytesSetting() 
    {
        GraphDatabaseSetting.NumberOfBytesSetting setting = new GraphDatabaseSetting.NumberOfBytesSetting("mysize");
        
        Config config = mock(Config.class);
        
        assertValidationPasses(setting, "1");
        assertValidationPasses(setting, "23");
        assertValidationPasses(setting, "12G");
        assertValidationPasses(setting, "12 g");
        assertValidationPasses(setting, "12 G");
        
        assertValidationFails(setting, null);
        assertValidationFails(setting, "");
        assertValidationFails(setting, "asd");
        
        assertThat(setting.valueOf("12", config), equalTo(12l));
        assertThat(setting.valueOf("12k", config), equalTo(12l * 1024));
        assertThat(setting.valueOf("12m", config), equalTo(12l * 1024 * 1024));
        assertThat(setting.valueOf("12g", config), equalTo(12l * 1024 * 1024 * 1024));
        assertThat(setting.valueOf("12 g", config), equalTo(12l * 1024 * 1024 * 1024));
        
    }
    
    @Test
    public void testIntegerNumberOfBytesSetting() throws Exception
    {
        Config config = mock( Config.class );
        
        GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting withoutMin =
                new GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting( "mysize" );
        assertValidationPasses( withoutMin, "1" );
        assertValidationPasses( withoutMin, "100k" );
        assertValidationPasses( withoutMin, "100M" );
        assertValidationPasses( withoutMin, "1G" );
        assertValidationFails( withoutMin, "" + (Integer.MAX_VALUE + 1) );
        assertValidationFails( withoutMin, "3g" );
        assertThat( withoutMin.valueOf( "2 g", config ), equalTo( 2 * 1024 * 1024 * 1024 ) );

        GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting withMin =
                new GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting( "mysize", 10 * 1024 );
        assertValidationFails( withMin, "1" );
        assertValidationFails( withMin, "9k" );
        assertValidationPasses( withMin, "10k" );
        assertValidationPasses( withMin, "100k" );
        assertValidationPasses( withMin, "1G" );
        assertValidationFails( withMin, "" + (Integer.MAX_VALUE + 1) );
        assertValidationFails( withMin, "3g" );
        assertThat( withMin.valueOf( "2 g", config ), equalTo( 2 * 1024 * 1024 * 1024 ) );
    }

	private void assertValidationPasses(GraphDatabaseSetting<?> setting,
			String value) 
	{
		setting.validate(value);
	}

	private void assertValidationFails(GraphDatabaseSetting<?> setting,
			String value) {
		try {
			setting.validate(value);
			fail("Expected validation of value " + (value == null?"[null]":"'"+value+"'") + " to fail.");
		} catch(IllegalArgumentException e){
			// Ok
		}
	}
}