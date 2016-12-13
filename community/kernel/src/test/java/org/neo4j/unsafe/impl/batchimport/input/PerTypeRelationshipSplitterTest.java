/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper.wrap;

public class PerTypeRelationshipSplitterTest
{
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( random )
                                          .around( directory ).around( fileSystemRule );

    @Test
    public void shouldReturnTypesOneByOne() throws Exception
    {
        // GIVEN
        Collection<InputRelationship> expected = randomRelationships();
        InputIterable<InputRelationship> relationships = wrap( "test", expected );
        InputCache inputCache = new InputCache( fileSystemRule.get(), directory.directory(),
                Standard.LATEST_RECORD_FORMATS, Configuration.DEFAULT );
        PerTypeRelationshipSplitter perType = new PerTypeRelationshipSplitter( relationships.iterator(),
                types( expected ), type -> false, type -> Integer.parseInt( type.toString() ), inputCache );

        // WHEN
        Set<Object> all = new HashSet<>();
        while ( perType.hasNext() )
        {
            try ( InputIterator<InputRelationship> relationshipsOfThisType = perType.next() )
            {
                // THEN
                Object type = perType.currentType();
                Collection<Object> expectedRelationshipsOfThisType = nodesOf( filter(
                        relationship -> relationship.typeAsObject().equals( type ), expected.iterator() ) );
                assertEquals( expectedRelationshipsOfThisType, nodesOf( relationshipsOfThisType ) );
                all.addAll( expectedRelationshipsOfThisType );
            }
        }

        assertEquals( nodesOf( expected.iterator() ), all );
        inputCache.close();
    }

    /**
     * Get the nodes of the relationships. We use those to identify relationships, since they have no ID
     * and no equals method (which they don't really need).
     *
     * @param relationship {@link InputRelationship} to get node ids from.
     * @return {@link Collection} of node ids from {@link InputRelationship} relationships.
     */
    private Collection<Object> nodesOf( Iterator<InputRelationship> relationship )
    {
        Collection<Object> nodes = new HashSet<>();
        while ( relationship.hasNext() )
        {
            nodes.add( relationship.next().startNode() );
        }
        return nodes;
    }

    private Object[] types( Collection<InputRelationship> expected )
    {
        Set<Object> types = new HashSet<>();
        for ( InputRelationship relationship : expected )
        {
            types.add( relationship.typeAsObject() );
        }
        return types.toArray();
    }

    private Collection<InputRelationship> randomRelationships()
    {
        Collection<InputRelationship> result = new ArrayList<>();
        int count = 100;
        Group group = Group.GLOBAL;
        boolean typeIds = random.nextBoolean();
        for ( int i = 0; i < count; i++ )
        {
            int typeId = random.nextInt( 5 );
            Object node = (long)i;
            InputRelationship relationship = new InputRelationship( "test", i, i, NO_PROPERTIES, null,
                    group, node, group, node,
                    typeIds ? null : String.valueOf( typeId ),
                    typeIds ? typeId : null );
            result.add( relationship );
        }
        return result;
    }
}
