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

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;

public class IndexCreatorImpl implements IndexCreator
{
    private final Collection<String> propertyKeys;
    private final Label label;
    private final InternalSchemaActions actions;

    public IndexCreatorImpl( InternalSchemaActions actions, Label label )
    {
        this( actions, label, new ArrayList<>() );
    }

    private IndexCreatorImpl( InternalSchemaActions actions, Label label, Collection<String> propertyKeys )
    {
        this.actions = actions;
        this.label = label;
        this.propertyKeys = propertyKeys;

        assertInUnterminatedTransaction();
    }

    @Override
    public IndexCreator on( String propertyKey )
    {
        assertInUnterminatedTransaction();
        return new IndexCreatorImpl( actions, label, copyAndAdd( propertyKeys, propertyKey) );
    }

    @Override
    public IndexDefinition create() throws ConstraintViolationException
    {
        assertInUnterminatedTransaction();

        if ( propertyKeys.isEmpty() )
        {
            throw new ConstraintViolationException( "An index needs at least one property key to index" );
        }

        return actions.createIndexDefinition( label, propertyKeys.toArray( new String[0] ) );
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInOpenTransaction();
    }

    private Collection<String> copyAndAdd( Collection<String> propertyKeys, String propertyKey )
    {
        Collection<String> ret = new ArrayList<>( propertyKeys );
        ret.add( propertyKey );
        return ret;
    }
}
