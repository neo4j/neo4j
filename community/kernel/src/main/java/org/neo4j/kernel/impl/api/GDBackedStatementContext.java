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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.IllegalLabelNameException;
import org.neo4j.kernel.api.LabelNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyNotFoundException;
import org.neo4j.kernel.impl.core.PropertyIndexManager;

public class GDBackedStatementContext implements StatementContext
{

    private static final String LABEL_KEY_PREFIX = "____LABEL____:";

    private final PropertyIndexManager propertyIndexManager;

    public GDBackedStatementContext( PropertyIndexManager propertyIndexManager )
    {
        this.propertyIndexManager = propertyIndexManager;
    }

    @Override
    public long getOrCreateLabelId( String label ) throws IllegalLabelNameException
    {
        // Validate label name
        if(label.length() == 0)
        {
            throw new IllegalLabelNameException( label );
        }

        return propertyIndexManager.getOrCreateId( labelToPropertyName( label ) );
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundException
    {
        try
        {
            return propertyIndexManager.getIdByKeyName( labelToPropertyName( label ) );
        }
        catch ( KeyNotFoundException e )
        {
            throw new LabelNotFoundException( label );
        }
    }

    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        //propertyIndexManager.getKeyById( (int)labelId ).getKey();

    }

    private String labelToPropertyName( String label )
    {
        return LABEL_KEY_PREFIX + label;
    }
}
