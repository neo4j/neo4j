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
package org.neo4j.index.lucene;

/**
 * ValueContext allows you to give not just a value, but to give the value
 * some context to live in.
 */
public class ValueContext
{
    private final Object value;
    private boolean indexNumeric;

    public ValueContext( Object value )
    {
        this.value = value;
    }
    
    /**
     * @return the value object specified in the constructor.
     */
    public Object getValue()
    {
        return value;
    }

    /**
     * Returns a ValueContext to be used with
     * {@link org.neo4j.graphdb.index.Index#add(org.neo4j.graphdb.PropertyContainer, String, Object)}
     *
     * @return a numeric ValueContext
     */
    public ValueContext indexNumeric()
    {
        if ( !( this.value instanceof Number ) )
        {
            throw new IllegalStateException( "Value should be a Number, is " + value +
                    " (" + value.getClass() + ")" );
        }
        this.indexNumeric = true;
        return this;
    }
    
    /**
     * Returns the string representation of the value given in the constructor,
     * or the unmodified value if {@link #indexNumeric()} has been called.
     * 
     * @return the, by the user, intended value to index.
     */
    public Object getCorrectValue()
    {
        return this.indexNumeric ? this.value : this.value.toString();
    }
    
    @Override
    public String toString()
    {
        return value.toString();
    }

    /**
     * Convience method to add a numeric value to an index.
     * @param value The value to add
     * @return A ValueContext that can be used with
     * {@link org.neo4j.graphdb.index.Index#add(org.neo4j.graphdb.PropertyContainer, String, Object)}
     */
    public static ValueContext numeric(Number value)
    {
        return new ValueContext(value).indexNumeric();
    }
}
