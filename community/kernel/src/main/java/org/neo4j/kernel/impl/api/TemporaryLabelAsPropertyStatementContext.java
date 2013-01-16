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

import org.neo4j.kernel.api.LabelNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyNotFoundException;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

// TODO This is the hack where we temporarily store the labels in the property store
public class TemporaryLabelAsPropertyStatementContext implements StatementContext
{
    private static final String LABEL_PREFIX = "___label___";

    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;

    public TemporaryLabelAsPropertyStatementContext( PropertyIndexManager propertyIndexManager,
                                            PersistenceManager persistenceManager )
    {
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
    }

    @Override
    public long getOrCreateLabelId( String label )
    {
        return propertyIndexManager.getOrCreateId( internalLabelName( label ) );
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundException
    {
        try
        {
            return propertyIndexManager.getIdByKeyName( label );
        }
        catch ( KeyNotFoundException e )
        {
            throw new LabelNotFoundException( label, e );
        }
    }

    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        PropertyIndex propertyIndex = propertyIndexManager.getKeyByIdOrNull( (int) labelId );
        persistenceManager.nodeAddProperty( nodeId, propertyIndex, new LabelAsProperty( nodeId ) );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        try
        {
            ArrayMap<Integer, PropertyData> propertyMap = persistenceManager.loadNodeProperties( nodeId, true );
            if ( propertyMap == null )
                return false;

            return propertyMap.get( (int) labelId ) != null;
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }

    private String internalLabelName( String label )
    {
        return LABEL_PREFIX + label;
    }

    @Override
    public void close()
    {
    }
}
