/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FulltextIndexProviderTest
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldProvideFulltextIndexProviderForFulltextIndexDescriptor()
    {
        AllByPrioritySelectionStrategy<IndexProvider<?>> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
        IndexProvider defaultIndexProvider = db.getDependencyResolver().resolveDependency( IndexProvider.class, indexProviderSelection );

        IndexProviderMap indexProviderMap = new DefaultIndexProviderMap( defaultIndexProvider, indexProviderSelection.lowerPrioritizedCandidates() );
        FulltextIndexDescriptor fulltextIndexDescriptor = new FulltextIndexDescriptor( FulltextIndexDescriptor.NO_LABEL, 1, 2, 3, 4 );
        assertThat( indexProviderMap.getProviderFor( fulltextIndexDescriptor ), is( instanceOf( FulltextIndexProvider.class ) ) );
    }
}
