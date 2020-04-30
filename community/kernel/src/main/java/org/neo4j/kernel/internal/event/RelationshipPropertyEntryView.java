/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.internal.event;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

class RelationshipPropertyEntryView implements PropertyEntry<Relationship>
{
    static final long SHALLOW_SIZE = shallowSizeOfInstance( RelationshipPropertyEntryView.class );

    private final Relationship relationship;
    private final String key;
    private final Value newValue;
    private final Value oldValue;

    RelationshipPropertyEntryView( Relationship relationship, String key, Value newValue, Value oldValue )
    {
        this.relationship = relationship;
        this.key = key;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    @Override
    public Relationship entity()
    {
        return relationship;
    }

    @Override
    public String key()
    {
        return key;
    }

    @Override
    public Object previouslyCommittedValue()
    {
        return oldValue.asObjectCopy();
    }

    @Override
    public Object value()
    {
        if ( newValue == null || newValue == Values.NO_VALUE )
        {
            throw new IllegalStateException( "This property has been removed, it has no value anymore: " + this );
        }
        return newValue.asObjectCopy();
    }

    @Override
    public String toString()
    {
        return "RelationshipPropertyEntryView{" +
                "relId=" + relationship.getId() +
                ", key='" + key + '\'' +
                ", newValue=" + newValue +
                ", oldValue=" + oldValue +
                '}';
    }
}
