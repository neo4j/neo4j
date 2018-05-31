/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class TxStateVisitorTest
{
    @Test
    public void shouldSeeAddedRelationshipProperties() throws Exception
    {
        // Given
        long relId = 1L;
        int propKey = 2;
        GatheringVisitor visitor = new GatheringVisitor();
        Value value = Values.of( "hello" );
        state.relationshipDoReplaceProperty( relId, propKey, Values.of( "" ), value );

        // When
        state.accept( visitor );

        // Then
        StorageProperty prop = new PropertyKeyValue( propKey, Values.of( "hello" ) );
        assertThat( visitor.relPropertyChanges, contains( propChange( relId, noProperty, asList( prop ), IntSets.immutable.empty() ) ) );
    }

    private Matcher<List<GatheringVisitor.PropertyChange>> contains( GatheringVisitor.PropertyChange ... change )
    {
        return equalTo(asList( change ));
    }

    private GatheringVisitor.PropertyChange propChange( long relId, Collection<StorageProperty> added,
            List<StorageProperty> changed, IntIterable removed )
    {
        return new GatheringVisitor.PropertyChange( relId, added, changed, removed );
    }

    private TransactionState state;
    private final Collection<StorageProperty> noProperty = Collections.emptySet();

    @Before
    public void before()
    {
        state = new TxState();
    }

    static class GatheringVisitor extends TxStateVisitor.Adapter
    {
        static class PropertyChange
        {
            final long entityId;
            final List<StorageProperty> added;
            final List<StorageProperty> changed;
            final IntList removed;

            PropertyChange( long entityId, Collection<StorageProperty> added, Collection<StorageProperty> changed,
                    IntIterable removed )
            {
                this.entityId = entityId;
                this.added = Iterables.asList(added);
                this.changed = Iterables.asList(changed);
                this.removed = removed.toList();
            }

            PropertyChange( long entityId, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                    IntIterable removed )
            {
                this.entityId = entityId;
                this.added = Iterators.asList(added);
                this.changed = Iterators.asList(changed);
                this.removed = removed.toList();
            }

            @Override
            public String toString()
            {
                return "PropertyChange{" +
                        "entityId=" + entityId +
                        ", added=" + added +
                        ", changed=" + changed +
                        ", removed=" + removed +
                        '}';
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                PropertyChange that = (PropertyChange) o;

                if ( entityId != that.entityId )
                {
                    return false;
                }
                if ( !added.equals( that.added ) )
                {
                    return false;
                }
                if ( !changed.equals( that.changed ) )
                {
                    return false;
                }
                return removed.equals( that.removed );
            }

            @Override
            public int hashCode()
            {
                int result = (int) (entityId ^ (entityId >>> 32));
                result = 31 * result + added.hashCode();
                result = 31 * result + changed.hashCode();
                result = 31 * result + removed.hashCode();
                return result;
            }
        }

        public List<PropertyChange> nodePropertyChanges = new ArrayList<>();
        public List<PropertyChange> relPropertyChanges = new ArrayList<>();
        public List<PropertyChange> graphPropertyChanges = new ArrayList<>();

        @Override
        public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty>
                changed, IntIterable removed )
        {
            nodePropertyChanges.add( new PropertyChange( id, added, changed, removed ) );
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty>
                changed, IntIterable removed )
        {
            relPropertyChanges.add( new PropertyChange( id, added, changed, removed ) );
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                IntIterable removed )
        {
            graphPropertyChanges.add( new PropertyChange( -1, added, changed, removed ) );
        }
    }
}
