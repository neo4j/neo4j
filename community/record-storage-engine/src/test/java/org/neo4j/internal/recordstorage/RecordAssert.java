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
package org.neo4j.internal.recordstorage;

import org.assertj.core.api.AbstractAssert;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.eclipse.collections.impl.block.factory.Functions.identity;
import static org.neo4j.internal.recordstorage.RecordBuilders.filterType;
import static org.neo4j.internal.recordstorage.RecordBuilders.records;

public class RecordAssert extends AbstractAssert<RecordAssert,RecordChangeSet>
{
    public RecordAssert( RecordChangeSet recordChangeSet )
    {
        super( recordChangeSet, RecordAssert.class );
    }

    public static RecordAssert assertThat( RecordChangeSet changeSet )
    {
        return new RecordAssert( changeSet );
    }

    /** Match a RecordChangeSet */
    public DiffAssert<RecordChangeSet> containsChanges( AbstractBaseRecord... expectedChanges )
    {
        DiffAssert<Iterable<? extends AbstractBaseRecord>> nodes = containsRecords( "nodes", filterType( expectedChanges, NodeRecord.class ) );
        DiffAssert<Iterable<? extends AbstractBaseRecord>> rels = containsRecords( "relationships", filterType( expectedChanges, RelationshipRecord.class ) );
        DiffAssert<Iterable<? extends AbstractBaseRecord>> groups =
                containsRecords( "relationship groups", filterType( expectedChanges, RelationshipGroupRecord.class ) );

        return new DiffAssert<>()
        {
            @Override
            String diff( RecordChangeSet actual )
            {
                String diff;

                diff = nodes.diff( records( actual.getNodeRecords().changes() ) );
                if ( diff != null )
                {
                    return diff;
                }

                diff = rels.diff( records( actual.getRelRecords().changes() ) );
                if ( diff != null )
                {
                    return diff;
                }

                diff = groups.diff( records( actual.getRelGroupRecords().changes() ) );
                return diff;
            }
        };
    }

    // Build a contains matcher that matches all records of a single given type
    // NOTE: This is a bit brittle, if you like it'd be easy to make it a general purpose
    // list-of-records-of-any-type matcher. As-is, if you use it to match mixed-type records,
    // behavior is undefined.
    // NOTE: This nests diff functions for individual records; if you want a matcher for
    // a single record, just refactor those out and have this delegate to them, see how
    // the containsChanges delegates here for an example.
    public DiffAssert<Iterable<? extends AbstractBaseRecord>> containsRecords(
            String recordPlural, Stream<? extends AbstractBaseRecord> expected )
    {
        Map<Long,AbstractBaseRecord> expectedById = expected.collect( toMap( AbstractBaseRecord::getId, identity() ) );
        return new DiffAssert<>()
        {
            @Override
            String diff( Iterable<? extends AbstractBaseRecord> actual )
            {
                Set<Long> seen = new HashSet<>( expectedById.keySet() );
                for ( AbstractBaseRecord record : actual )
                {
                    seen.remove( record.getId() );
                    if ( !expectedById.containsKey( record.getId() ) )
                    {
                        return String.format( "This record was not expected: %s", record );
                    }

                    String diff = diff( expectedById.get( record.getId() ), record );
                    if ( diff != null )
                    {
                        return diff;
                    }
                }

                return null;
            }

            private String diff( AbstractBaseRecord expected, AbstractBaseRecord actual )
            {
                if ( expected instanceof NodeRecord )
                {
                    return diff( (NodeRecord) expected, (NodeRecord) actual );
                }
                if ( expected instanceof RelationshipRecord )
                {
                    return diff( (RelationshipRecord) expected, (RelationshipRecord) actual );
                }
                if ( expected instanceof RelationshipGroupRecord )
                {
                    return diff( (RelationshipGroupRecord) expected, (RelationshipGroupRecord) actual );
                }
                throw new UnsupportedOperationException( String.format( "No diff implementation (just add one, its easy) for: %s", expected ) );
            }

            private String diff( NodeRecord expected, NodeRecord actual )
            {
                if ( actual.getId() == expected.getId() && actual.getNextRel() == expected.getNextRel() && actual.getLabelField() == expected.getLabelField() &&
                        actual.getNextProp() == expected.getNextProp() && actual.isDense() == expected.isDense() && actual.isLight() == expected.isLight() )
                {
                    return null;
                }
                return describeDiff( expected.toString(), actual.toString() );
            }

            private String diff( RelationshipGroupRecord expected, RelationshipGroupRecord actual )
            {
                if ( actual.getId() == expected.getId() && actual.getType() == expected.getType() && actual.getNext() == expected.getNext() &&
                        actual.getFirstOut() == expected.getFirstOut() && actual.getFirstIn() == expected.getFirstIn() &&
                        actual.getFirstLoop() == expected.getFirstLoop() && actual.getOwningNode() == expected.getOwningNode() )
                {
                    return null;
                }
                return describeDiff( expected.toString(), actual.toString() );
            }

            private String diff( RelationshipRecord expected, RelationshipRecord actual )
            {
                if ( actual.getId() == expected.getId() && actual.getFirstNode() == expected.getFirstNode() &&
                        actual.getSecondNode() == expected.getSecondNode() && actual.getType() == expected.getType() &&
                        actual.getFirstPrevRel() == expected.getFirstPrevRel() && actual.getFirstNextRel() == expected.getFirstNextRel() &&
                        actual.getSecondPrevRel() == expected.getSecondPrevRel() && actual.getSecondNextRel() == expected.getSecondNextRel() &&
                        actual.isFirstInFirstChain() == expected.isFirstInFirstChain() && actual.isFirstInSecondChain() == expected.isFirstInSecondChain() )
                {
                    return null;
                }
                return describeDiff( expected.toString(), actual.toString() );
            }

            private String describeDiff( String expected, String actual )
            {
                StringBuilder arrow = new StringBuilder();
                char[] expectedChars = expected.toCharArray();
                char[] actualChars = actual.toCharArray();
                for ( int i = 0; i < Math.min( expectedChars.length, actualChars.length ); i++ )
                {
                    if ( expectedChars[i] != actualChars[i] )
                    {
                        break;
                    }
                    arrow.append( "-" );
                }
                return String.format( "Record fields don't match.\n" + "Expected: %s\n" + "Actual:   %s\n" + "          %s", expected, actual,
                        arrow.append( "^" ).toString() );
            }
        };
    }

    // Matcher where you implement a common "diff" describer, which fails if the
    // diff is non-null. Benefit here being that you don't have to duplicate the
    // match logic in the mismatch description; you write one function to find difference
    // and get both match and describeMismatch implemented for you.
    public abstract class DiffAssert<T>
    {
        abstract String diff( T item );

        protected void hasDiff( T item )
        {
            var itemDifference = diff( item );
            if ( isNotBlank( itemDifference ) )
            {
                failWithMessage( "Element difference found: " + itemDifference );
            }
        }
    }
}
