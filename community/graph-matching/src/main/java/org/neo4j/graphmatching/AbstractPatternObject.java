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
package org.neo4j.graphmatching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * The base class for {@link PatternNode} and {@link PatternRelationship}.
 *
 * @param <T> either {@link Node} or {@link Relationship}.
 */
@Deprecated
public abstract class AbstractPatternObject<T extends PropertyContainer>
{
    private T assocication;
    private Map<String, Collection<ValueMatcher>> constrains =
            new HashMap<String, Collection<ValueMatcher>>();
    protected String label;

    AbstractPatternObject()
    {
    }

    /**
     * Add a constraint to the property with the given key on this pattern
     * object.
     *
     * @param propertyKey the key of the property to add a constraint to.
     * @param matcher the constraint to add on the property.
     */
    public void addPropertyConstraint( String propertyKey, ValueMatcher matcher )
    {
        Collection<ValueMatcher> matchers = this.constrains.get( propertyKey );
        if ( matchers == null )
        {
            matchers = new ArrayList<ValueMatcher>();
            this.constrains.put( propertyKey, matchers );
        }
        matchers.add( matcher );
    }

    /**
     * Associate this object with a particular {@link Node} or
     * {@link Relationship}. When a pattern object is associated with an actual
     * object it will only match that object. Set the association to
     * <code>null</code> to remove the association.
     *
     * @param object the {@link Node} or {@link Relationship} to associate this
     *            pattern object with.
     */
    public void setAssociation( T object )
    {
        this.assocication = object;
    }

    /**
     * Get the {@link Node} or {@link Relationship} currently associated with
     * this pattern object.
     *
     * @return the {@link Node} or {@link Relationship} associated with this
     *         pattern object.
     */
    public T getAssociation()
    {
        return this.assocication;
    }

    /**
     * Get all the constraints on the properties of this pattern object.
     * 
     * @return an iterable of all constrained properties with all constraints
     *         for each of them.
     */
    public Iterable<Map.Entry<String, Collection<ValueMatcher>>> getPropertyConstraints()
    {
        Iterable<Map.Entry<String, Collection<ValueMatcher>>> matchers = this.constrains.entrySet();
        return matchers != null ? matchers :
                Collections.<Map.Entry<String, Collection<ValueMatcher>>>emptyList();
    }

    /**
     * Get the label of this pattern object.
     *
     * @return the label of this pattern object.
     */
    public String getLabel() {
        return label;
    }

    /**
     * Sets the label of this pattern object;
     * @param label the label of this pattern object;
     */
    public void setLabel(String label) {
        this.label = label;
    }
}
