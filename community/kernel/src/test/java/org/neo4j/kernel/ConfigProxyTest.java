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

package org.neo4j.kernel;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

/**
 * ConfigProxy tests
 */
public class ConfigProxyTest
{
    public enum Options
    {
        first,
        second
    }
    
    public interface Configuration
    {
        float floatNoDef();
        float floatValueMinMax(float def, float min, float max);
        double doubleValueMinMax(double def, double min, double max);
        long longValueMinMax(long def, long min, long max);
        int integerValueMinMax(int def, int min, int max);
        
        boolean boolDefined();
        boolean boolNotDefined();
        boolean boolNotDefinedWithDefault(boolean def);
        
        Options someOption();
        Options someOptionWithDefault(Options def);
    }
    
    @ConfigurationPrefix( "test." )
    public interface ConfigurationPrefixed
    {
        String foo();
    }
    
    @ConfigurationPrefix( "test2." )
    public interface ConfigurationPrefixed2
        extends ConfigurationPrefixed
    {
        String bar();
    }

    @Test
    public void testNumbersAndRanges()
    {
        Map<String,String> map = new HashMap<String, String>();
        map.put("floatValueMinMax", "3.0");
        map.put("doubleValueMinMax", "3.0");
        map.put("longValueMinMax", "3");
        map.put("integerValueMinMax", "3");
        
        Configuration conf = ConfigProxy.config(map, Configuration.class);
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), equalTo( 3.0F ));
        Assert.assertThat(conf.floatValueMinMax(4, 5, 7), equalTo( 5.0F ));
        Assert.assertThat(conf.floatValueMinMax(4, 1, 2), equalTo( 2.0F ));

        Assert.assertThat(conf.doubleValueMinMax(4, 1, 5), equalTo( 3.0D ));
        Assert.assertThat(conf.doubleValueMinMax(4, 5, 7), equalTo( 5.0D ));
        Assert.assertThat(conf.doubleValueMinMax(4, 1, 2), equalTo( 2.0D ));

        Assert.assertThat(conf.longValueMinMax(4, 1, 5), equalTo( 3L ));
        Assert.assertThat(conf.longValueMinMax(4, 5, 7), equalTo( 5L ));
        Assert.assertThat(conf.longValueMinMax(4, 1, 2), equalTo( 2L ));

        Assert.assertThat(conf.integerValueMinMax(4, 1, 5), equalTo( 3 ));
        Assert.assertThat(conf.integerValueMinMax(4, 5, 7), equalTo( 5 ));
        Assert.assertThat(conf.integerValueMinMax(4, 1, 2), equalTo( 2 ));

        // Invalid number format
        map.put("floatValueMinMax", "3x");
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), equalTo( 4.0F ));
        map.put("floatValueMinMax", "3,0");
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), equalTo( 4.0F ));
        map.put("floatNoDef", "3,0");
        try
        {
            conf.floatNoDef();
            Assert.fail("Should have thrown exception");
        } catch (Exception e)
        {
            // Ok!
        }
    }
    
    @Test
    public void testBoolean()
    {
        Map<String,String> map = new HashMap<String, String>();
        map.put("boolDefined", "true");
        
        Configuration conf = ConfigProxy.config(map, Configuration.class);
        Assert.assertThat(conf.boolDefined(), equalTo( true ));
        map.put("boolDefined", "TrUe");
        Assert.assertThat(conf.boolDefined(), equalTo( true ));

        try
        {
            conf.boolNotDefined();
            Assert.fail();
        } catch (Exception e)
        {
        }

        Assert.assertThat(conf.boolNotDefinedWithDefault(true), equalTo( true ));
    }
    
    @Test
    public void testEnum()
    {
        Map<String,String> map = new HashMap<String, String>();
        map.put("someOption", "first");
        
        Configuration conf = ConfigProxy.config(map, Configuration.class);
        Assert.assertThat(conf.someOption(), equalTo( Options.first ));

        map.put("someOption", "third");
        try
        {
            conf.someOption();
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        map.put("someOptionWithDefault", "third");
        Assert.assertThat(conf.someOptionWithDefault( Options.first ), equalTo( Options.first ));

        map.remove( "someOption" );
        try
        {
            conf.someOption();
            Assert.fail();
        } catch (Exception e)
        {
        }
    }
    
    @Test
    public void testPrefix()
    {
        ConfigurationPrefixed conf = ConfigProxy.config( MapUtil.stringMap( "test.foo", "bar" ), ConfigurationPrefixed.class );
        Assert.assertThat(conf.foo(), equalTo( "bar" ));

        ConfigurationPrefixed2 conf2 = ConfigProxy.config( MapUtil.stringMap( "test.foo", "bar", "test2.bar", "foo" ), ConfigurationPrefixed2.class );
        Assert.assertThat(conf2.foo(), equalTo( "bar" ));
        Assert.assertThat(conf2.bar(), equalTo( "foo" ));

    }
}
