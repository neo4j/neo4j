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

/**
* Represents a change to the database configuration.
*/
public final class ConfigurationChange
{
    private String name;
    private String oldValue;
    private String newValue;

    public ConfigurationChange( String name, String oldValue, String newValue )
    {
        this.name = name;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public String getName()
    {
        return name;
    }

    public String getOldValue()
    {
        return oldValue;
    }

    public String getNewValue()
    {
        return newValue;
    }

    @Override
    public String toString()
    {
        return name+":"+oldValue+"->"+newValue;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ConfigurationChange) )
        {
            return false;
        }

        ConfigurationChange that = (ConfigurationChange) o;

        return name.equals( that.name ) &&
               !(newValue != null ? !newValue.equals( that.newValue ) : that.newValue != null) &&
               !(oldValue != null ? !oldValue.equals( that.oldValue ) : that.oldValue != null);

    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
        result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
        return result;
    }
}
