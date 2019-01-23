/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.pagecache.randomharness;

abstract class Action
{
    private final Command command;
    private final String format;
    private final Object[] parameters;
    private final Action innerAction;

    protected Action( Command command, String format, Object... parameters )
    {
        this( command, null, format, parameters );
    }

    protected Action( Command command, Action innerAction, String format, Object... parameters )
    {
        this.command = command;
        this.format = format;
        this.parameters = parameters;
        this.innerAction = innerAction;
    }

    abstract void perform() throws Exception;

    protected void performInnerAction() throws Exception
    {
        if ( innerAction != null )
        {
            innerAction.perform();
        }
    }

    @Override
    public String toString()
    {
        if ( innerAction == null )
        {
            return String.format( command + format, parameters );
        }
        else
        {
            return String.format( command + format + ", and then " + innerAction, parameters );
        }
    }
}
