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
package org.neo4j.kernel.impl.api.state;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class TxStateVisitorTest
{
    @Test
    public void shouldSeeAddedRelationshipProperties() throws Exception
    {
        // Given
        long relId = 1l;
        int propKey = 2;
        GatheringVisitor visitor = new GatheringVisitor();
        DefinedProperty prop = stringProperty( propKey, "hello" );
        state.relationshipDoReplaceProperty( relId, stringProperty( propKey, "" ), prop );

        // When
        state.accept( visitor );

        // Then
        assertThat(visitor.relPropertyChanges, contains( propChange( relId, noProperty, asList( prop ), noRemoved ) ) );
    }

    private Matcher<List<GatheringVisitor.PropertyChange>> contains( GatheringVisitor.PropertyChange ... change )
    {
        return equalTo(asList( change ));
    }

    private GatheringVisitor.PropertyChange propChange( long relId, Collection<DefinedProperty> added, List<DefinedProperty> changed, Collection<Integer> removed )
    {
        return new GatheringVisitor.PropertyChange( relId, added, changed, removed );
    }


    private TransactionState state;
    private final Collection<DefinedProperty> noProperty = Collections.emptySet();
    private final Collection<Integer> noRemoved = Collections.emptySet();

    @Before
    public void before() throws Exception
    {
        state = new TxState();
    }

    static class GatheringVisitor extends TxStateVisitor.Adapter
    {
        static class PropertyChange
        {
            final long entityId;
            final List<DefinedProperty> added;
            final List<DefinedProperty> changed;
            final List<Integer> removed;

            PropertyChange( long entityId, Collection<DefinedProperty> added, Collection<DefinedProperty> changed, Collection<Integer>
                    removed )
            {
                this.entityId = entityId;
                this.added = IteratorUtil.asList(added);
                this.changed = IteratorUtil.asList(changed);
                this.removed = IteratorUtil.asList(removed);
            }

            PropertyChange( long entityId, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed, Iterator<Integer>
                    removed )
            {
                this.entityId = entityId;
                this.added = IteratorUtil.asList(added);
                this.changed = IteratorUtil.asList(changed);
                this.removed = IteratorUtil.asList(removed);
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
                if ( !removed.equals( that.removed ) )
                {
                    return false;
                }

                return true;
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
        public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty>
                changed, Iterator<Integer> removed )
        {
            nodePropertyChanges.add( new PropertyChange( id, added, changed, removed ) );
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty>
                changed, Iterator<Integer> removed )
        {
            relPropertyChanges.add( new PropertyChange( id, added, changed, removed ) );
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                               Iterator<Integer> removed )
        {
            graphPropertyChanges.add( new PropertyChange( -1, added, changed, removed ) );
        }
    }
}
