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
package org.neo4j.graphdb.config;

public class InvalidConfigurationValueException extends Exception
{
    private final Setting setting;
    private final String value;
    private final String message;

    public InvalidConfigurationValueException( Setting setting, String value, String message )
    {
        super( String.format( "\"%s\" is not a valid value for '%s': %s", value, setting.name(), message ) );
        this.setting = setting;
        this.value = value;
        this.message = message;
    }

    public InvalidConfigurationValueException( Setting setting, String value, Exception cause )
    {
        super( String.format( "\"%s\" is not a valid value for '%s': %s", value, setting.name(), cause.getMessage() ),
                cause );
        this.setting = setting;
        this.value = value;
        this.message = null;
    }
}
