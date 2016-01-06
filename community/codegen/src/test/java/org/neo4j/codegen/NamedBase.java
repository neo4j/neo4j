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
package org.neo4j.codegen;

@SuppressWarnings( "ALL" )
public class NamedBase
{
    final String name;
    private boolean defaultConstructorCalled = false;
    private String foo;
    private String bar;

    public NamedBase()
    {
        this.defaultConstructorCalled = true;
        this.name = null;
    }

    public NamedBase( String name )
    {
        this.name = name;
    }

    public boolean defaultConstructorCalled()
    {
        return defaultConstructorCalled;
    }

    public String getFoo()
    {
        return foo;
    }

    public String getBar()
    {
        return bar;
    }

    public void setFoo( String foo )
    {
        this.foo = foo;
    }

    public void setBar( String bar )
    {
        this.bar = bar;
    }
}
