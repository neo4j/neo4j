/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import java.util.Iterator;

import org.neo4j.driver.Identity;
import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.NotMultiValued;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.exceptions.value.Unsizable;

import static java.util.Collections.emptyList;

public abstract class ValueAdapter implements Value
{
    @Override
    public String javaString()
    {
        throw new Uncoercible( typeName(), "Java String" );
    }

    @Override
    public int javaInteger()
    {
        throw new Uncoercible( typeName(), "Java int" );
    }

    @Override
    public long javaLong()
    {
        throw new Uncoercible( typeName(), "Java long" );
    }

    @Override
    public float javaFloat()
    {
        throw new Uncoercible( typeName(), "Java float" );
    }

    @Override
    public double javaDouble()
    {
        throw new Uncoercible( typeName(), "Java double" );
    }

    @Override
    public boolean javaBoolean()
    {
        throw new Uncoercible( typeName(), "Java boolean" );
    }

    @Override
    public Identity asIdentity()
    {
        throw new Uncoercible( typeName(), "Identity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( typeName(), "Node" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( typeName(), "Path" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( typeName(), "Relationship" );
    }

    @Override
    public Value get( long index )
    {
        throw new NotMultiValued( typeName() + " is not an indexed collection" );
    }

    @Override
    public Value get( String key )
    {
        throw new NotMultiValued( typeName() + " is not a keyed collection" );
    }

    @Override
    public long size()
    {
        throw new Unsizable( typeName() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean isText()
    {
        return false;
    }

    @Override
    public boolean isInteger()
    {
        return false;
    }

    @Override
    public boolean isFloat()
    {
        return false;
    }

    @Override
    public boolean isBoolean()
    {
        return false;
    }

    @Override
    public boolean isIdentity()
    {
        return false;
    }

    @Override
    public boolean isNode()
    {
        return false;
    }

    @Override
    public boolean isPath()
    {
        return false;
    }

    @Override
    public boolean isRelationship()
    {
        return false;
    }

    @Override
    public boolean isList()
    {
        return false;
    }

    @Override
    public boolean isMap()
    {
        return false;
    }

    @Override
    public Iterator<Value> iterator()
    {
        throw new NotMultiValued( typeName() + " is not iterable" );
    }

    @Override
    public String toString()
    {
        return String.format( "%s<>", typeName() );
    }

    protected String typeName()
    {
        if ( isFloat() ) { return "float"; }
        if ( isInteger() ) { return "integer"; }
        if ( isBoolean() ) { return "boolean"; }
        if ( isText() ) { return "text"; }
        if ( isList() ) { return "list"; }
        if ( isMap() ) { return "map"; }
        if ( isIdentity() ) { return "identity"; }
        if ( isNode() ) { return "node"; }
        if ( isRelationship() ) { return "relationship"; }
        if ( isPath() ) { return "path"; }
        return "unknown";
    }
}
