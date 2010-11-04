/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin.properties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.rest.domain.Representation;

/**
 * Defines meta-data about a configuration property value. This meta data is
 * used by the client to determine what type of widget (textbox, dropdown, etc.)
 * should be used to display the value.
 * 
 * It also allows for specifying append text and prepend text that should be put
 * before and after the value when it is in the config file, but that the used
 * should not see.
 * 
 * Append/prepend is used, for instance, to hide the "-Xmx" part of "-Xmx512M"
 * from the user, and simply having the user write "512M".
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ValueDefinition implements Representation
{

    public enum Widget
    {

        /**
         * Plain text input, this is the default.
         */
        TEXT,

        /**
         * A widget that switches between the default value and no value at all,
         * or between two default values.
         */
        TOGGLE,

        /**
         * A widget that shows a list of pre-defined values.
         */
        DROPDOWN
    }

    /**
     * Convinience method for converting a map into an arraylist of Map{value,
     * name}
     * 
     * @param values
     * @return
     */
    public static ArrayList<HashMap<String, String>> mapToValues(
            Map<String, String> values )
    {
        ArrayList<HashMap<String, String>> listValues = new ArrayList<HashMap<String, String>>();
        for ( String key : values.keySet() )
        {
            HashMap<String, String> value = new HashMap<String, String>();
            value.put( "name", key );
            value.put( "value", values.get( key ) );
            listValues.add( value );
        }
        return listValues;
    }

    /**
     * Convinience method for converting a list of values into an arraylist of
     * Map{value, name}. Name will be the same as values.
     * 
     * @param values
     * @return
     */
    public static ArrayList<HashMap<String, String>> itemsToValues(
            String... values )
    {
        ArrayList<HashMap<String, String>> listValues = new ArrayList<HashMap<String, String>>();
        for ( String value : values )
        {
            HashMap<String, String> mapValue = new HashMap<String, String>();
            mapValue.put( "name", value );
            mapValue.put( "value", value );
            listValues.add( mapValue );
        }
        return listValues;
    }

    private ArrayList<HashMap<String, String>> values;
    private String prepend;
    private String append;
    private Widget widget;

    //
    // CONSTRUCTORS
    //

    /**
     * Creates a text-box widget with no append/prepend strings.
     */
    public ValueDefinition()
    {
        this( "", "" );
    }

    /**
     * Creates a text-box widget
     * 
     * @param prepend
     * @param append
     */
    public ValueDefinition( String prepend, String append )
    {
        this( prepend, append, new HashMap<String, String>(), Widget.TEXT );
    }

    /**
     * Creates a toggle-widget that toggles between the value you define and no
     * value at all.
     * 
     * @param prepend
     * @param append
     * @param value
     */
    public ValueDefinition( String prepend, String append, String value )
    {
        this.prepend = prepend;
        this.values = itemsToValues( value );
        this.append = append;
        this.widget = Widget.TOGGLE;
    }

    /**
     * Creates a toggle-widget that toggles between two values.
     * 
     * @param prepend
     * @param append
     * @param value
     */
    public ValueDefinition( String prepend, String append, String firstValue,
            String secondValue )
    {
        this.prepend = prepend;
        this.values = itemsToValues( firstValue, secondValue );
        this.append = append;
        this.widget = Widget.TOGGLE;
    }

    /**
     * Creates a drop-down list.
     * 
     * @param prepend
     * @param append
     * @param values
     */
    public ValueDefinition( String prepend, String append,
            Map<String, String> values )
    {
        this( prepend, append, values, Widget.DROPDOWN );
    }

    public ValueDefinition( String prepend, String append,
            Map<String, String> values, Widget widget )
    {
        this.prepend = prepend;
        this.values = mapToValues( values );
        this.append = append;
        this.widget = widget;
    }

    //
    // PUBLIC
    //

    public Object serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "widget", this.widget );

        if ( this.widget != Widget.TEXT )
        {
            map.put( "values", this.values );
        }

        return map;
    }

    public String getPrepend()
    {
        return this.prepend;
    }

    public String getAppend()
    {
        return this.append;
    }

    /**
     * Take a user-entered value, and convert it to the full value that should
     * be written to the config file.
     * 
     * @param value
     * @return
     */
    public String toFullValue( String value )
    {
        if ( this.widget == Widget.TOGGLE && value.length() == 0 )
        {
            return "";
        }

        return this.prepend + value + this.append;
    }

    /**
     * Take a full configuration value, and remove any append/prepend stuff that
     * we don't want the user to see.
     * 
     * @param value
     * @return
     */
    public String fromFullValue( String value )
    {
        return value.replaceFirst( "^" + this.prepend, "" ).replaceFirst(
                this.append + "$", "" );
    }
}
