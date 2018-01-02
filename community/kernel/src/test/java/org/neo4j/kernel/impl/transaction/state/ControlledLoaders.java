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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;

/**
 * Can be used to mimic a store with use in a {@link RecordChangeSet}.
 */
public class ControlledLoaders
{
    private final ControlledLoader<Long,NodeRecord,Void> nodes = new ControlledLoader<>();
    private final ControlledLoader<Long,PropertyRecord,PrimitiveRecord> properties = new ControlledLoader<>();
    private final ControlledLoader<Long,RelationshipRecord,Void> relationships = new ControlledLoader<>();
    private final ControlledLoader<Long,RelationshipGroupRecord,Integer> relationshipGroups = new ControlledLoader<>();
    private final ControlledLoader<Long,Collection<DynamicRecord>,SchemaRule> schemaRules = new ControlledLoader<>();
    private final ControlledLoader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokens = new ControlledLoader<>();
    private final ControlledLoader<Integer,LabelTokenRecord,Void> labelTokens = new ControlledLoader<>();
    private final ControlledLoader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokens = new ControlledLoader<>();

    public Map<Long,NodeRecord> getNodes()
    {
        return nodes.data;
    }

    public Map<Long,PropertyRecord> getProperties()
    {
        return properties.data;
    }


    public Map<Long,RelationshipRecord> getRelationships()
    {
        return relationships.data;
    }


    public Map<Long,RelationshipGroupRecord> getRelationshipGroups()
    {
        return relationshipGroups.data;
    }

    public RecordAccessSet newAccessSet()
    {
        return new RecordChangeSet( false, nodes, properties, relationships, relationshipGroups, schemaRules,
                propertyKeyTokens, labelTokens, relationshipTypeTokens );
    }

    private static class ControlledLoader<KEY,RECORD,ADDITIONAL> implements Loader<KEY,RECORD,ADDITIONAL>
    {
        private final Map<KEY,RECORD> data = new HashMap<>();

        @Override
        public RECORD newUnused( KEY key, ADDITIONAL additionalData )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public RECORD load( KEY key, ADDITIONAL additionalData )
        {
            return data.get( key );
        }

        @Override
        public void ensureHeavy( RECORD record )
        {
            // Just assume everything is heavy already
        }

        @Override
        public RECORD clone( RECORD record )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }
    }
}
