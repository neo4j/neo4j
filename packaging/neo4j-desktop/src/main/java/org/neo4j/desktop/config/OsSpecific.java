/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

/**
 * Factory/utility for getting an OS-specific implementations for an interface or abstract class where
 * OS-specific functionality is required. Everything that isn't generic and applicable to all environments
 * should be encapsulated in a sub-class of OsSpecific, more specifically in one or more of the implementations
 * it hands out.
 */
public abstract class OsSpecific<T>
{
    public T get()
    {
        return getFor( figureOutOs() );
    }
    
    private Os figureOutOs()
    {
        return Os.WINDOWS;
    }

    protected abstract T getFor( Os os );
    
    public static enum Os
    {
        WINDOWS,
        LINUX,
        MAC;
    }
}
