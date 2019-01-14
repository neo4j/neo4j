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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.Arrays.asList;

public class IndexDefinitionImpl implements IndexDefinition
{
    private final InternalSchemaActions actions;

    private final Label label;
    private final String[] propertyKeys;
    private final boolean constraintIndex;

    public IndexDefinitionImpl( InternalSchemaActions actions, Label label, String[] propertyKeys,
            boolean constraintIndex )
    {
        this.actions = actions;
        this.label = label;
        this.propertyKeys = propertyKeys;
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
        return asList( propertyKeys );
    }

    @Override
    public void drop()
    {
        try
        {
            actions.dropIndexDefinitions( this );
        }
        catch ( ConstraintViolationException e )
        {
            if ( this.isConstraintIndex() )
            {
                throw new IllegalStateException( "Constraint indexes cannot be dropped directly, " +
                                                 "instead drop the owning uniqueness constraint.", e );
            }
            throw e;
        }
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
        return 31 * label.name().hashCode() + Arrays.hashCode( propertyKeys );
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
        return label.name().equals( other.label.name() ) && Arrays.equals( propertyKeys, other.propertyKeys );
    }

    @Override
    public String toString()
    {
        return "IndexDefinition[label:" + label + ", on:" +
               Arrays.stream( propertyKeys ).collect( Collectors.joining( "," ) ) + "]";
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInOpenTransaction();
    }
}
