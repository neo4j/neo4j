/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;

public class BatchIndexDefinitionImpl implements IndexDefinition
{
    private final Label label;
    private final Collection<String> propertyKeys;

    public BatchIndexDefinitionImpl( Label label, String... propertyKeys )
    {
        this.label = label;
        this.propertyKeys = new ArrayList<String>( );
        for ( String propertyKey : propertyKeys )
            this.propertyKeys.add( propertyKey );
    }

    @Override
    public Label getLabel()
    {
        return label;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return Collections.unmodifiableCollection( propertyKeys );
    }

    @Override
    public void drop()
    {
        throw new UnsupportedOperationException( "Dropping schema indexes is not supported in batch mode" );
    }
}
