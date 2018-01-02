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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.Arrays.asList;

public class IndexDefinitionImpl implements IndexDefinition
{
    private final InternalSchemaActions actions;

    private final Label label;
    private final String propertyKey;
    private final boolean constraintIndex;

    public IndexDefinitionImpl( InternalSchemaActions actions, Label label, String propertyKey,
                                boolean constraintIndex )
    {
        this.actions = actions;
        this.label = label;
        this.propertyKey = propertyKey;
        this.constraintIndex = constraintIndex;

        assertInUnterminatedTransaction();
    }

    @Override
    public Label getLabel()
    {
        assertInUnterminatedTransaction();
        return label;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return asList( propertyKey );
    }

    @Override
    public void drop()
    {
        // expected to call assertInUnterminatedTransaction()
        if ( this.isConstraintIndex() )
        {
            throw new IllegalStateException( "Constraint indexes cannot be dropped directly, " +
                                             "instead drop the owning uniqueness constraint." );
        }

        actions.dropIndexDefinitions( label, propertyKey );
    }

    @Override
    public boolean isConstraintIndex()
    {
        assertInUnterminatedTransaction();
        return constraintIndex;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + label.name().hashCode();
        result = prime * result + propertyKey.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
        return label.name().equals( other.label.name() ) && propertyKey.equals( other.propertyKey );
    }

    @Override
    public String toString()
    {
        return "IndexDefinition[label:" + label + ", on:" + propertyKey + "]";
    }

    protected void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
