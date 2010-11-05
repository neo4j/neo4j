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

package org.neo4j.server.webadmin.domain;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.domain.Representation;
import org.neo4j.server.webadmin.properties.ValueDefinition;

/**
 * Represents a server configuration setting. This is an abstraction of the
 * three types of settings that are possible:
 * 
 * <ul>
 * <li>Configuration file settings</li>
 * <li>JVM directives</li>
 * <li>Database creation settings</li>
 * </ul>
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ServerPropertyRepresentation implements Representation
{

    /**
     * The various types of properties that are available.
     * 
     * <ul>
     * <li>CONFIG_PROPERTY is a setting that ends up in the neo4j properties
     * file. Keys with this property type map directly to standard neo4j
     * properties.</li>
     * <li>JVM_ARGUMENT is a jvm command-line argument that will be passed
     * directly to the JVM</li>
     * <li>DB_CREATION_PROPERTY are properties that only apply when creating
     * neo4j databases</li>
     * <li>GENERAL_PROPERTY is any other property, it will not be magically
     * applied anywhere, but is to be used as a general key-value storage for
     * each neo4j server</li>
     * </ul>
     * 
     * @author Jacob Hansson <jacob@voltvoodoo.com>
     * 
     */
    public enum PropertyType
    {
        CONFIG_PROPERTY,
        JVM_ARGUMENT,
        APP_ARGUMENT,
        DB_CREATION_PROPERTY,
        GENERAL_PROPERTY
    }

    protected String key;
    protected String displayName;
    protected PropertyType type;
    protected ValueDefinition valueDefinition;
    protected String value;

    //
    // CONSTRUCTORS
    //

    public ServerPropertyRepresentation( String key, String value,
            PropertyType type )
    {
        this( key, key, value, type );
    }

    public ServerPropertyRepresentation( String key, String displayName,
            String value, PropertyType type )
    {
        this( key, displayName, value, type, new ValueDefinition() );
    }

    public ServerPropertyRepresentation( String key, String displayName,
            String value, PropertyType type, ValueDefinition valueDefinition )
    {
        this.key = key;
        this.displayName = displayName;
        this.value = value;
        this.type = type;
        this.valueDefinition = valueDefinition;
    }

    //
    // PUBLIC
    //

    public Object serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "key", this.key );
        map.put( "display_name", this.displayName );
        map.put( "type", this.type );
        map.put( "value", this.value );
        map.put( "definition", this.valueDefinition.serialize() );
        return map;
    }

    /**
     * Check if a given string is an okay value for this property.
     * 
     * TODO: Write actual implementation of this :)
     * 
     * @param value
     * @return
     */
    public boolean isValidValue( String value )
    {
        return true;
    }

    public PropertyType getType()
    {
        return type;
    }

    public String getKey()
    {
        return key;
    }

    public String getValue()
    {
        return value;
    }

    /**
     * Get the value of this property, including any prepend/append strings that
     * we normally don't want the user to see.
     * 
     * This is used when writing the value to configuration files.
     * 
     * @return
     */
    public String getFullValue()
    {
        return this.valueDefinition.toFullValue( this.value );
    }

    //
    // SETTERS
    //

    public void setValue( String value )
    {
        this.value = value;
    }

    /**
     * Set the full value of this property representation, including anything
     * that would normally be prepended or appended. This will strip off any
     * prepend/append stuff before setting the actual value internally.
     * 
     * It is meant to be used when setting a value that has been loaded directly
     * from a config file.
     * 
     * @param value
     */
    public void setFullValue( String value )
    {
        this.value = this.valueDefinition.fromFullValue( value );
    }

    public void setDisplayName( String name )
    {
        this.displayName = name;
    }
}
