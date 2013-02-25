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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class RuleUpdateFilterIndexContext extends DelegatingIndexContext
{
    private final IndexRule rule;

    private final Predicate<NodePropertyUpdate> ruleMatchingUpdates = new Predicate<NodePropertyUpdate>()
    {
        @Override
        public boolean accept( NodePropertyUpdate item )
        {
            return item.getPropertyKeyId() == rule.getPropertyKey() && item.forLabel( rule.getLabel() );
        }
    };

    public RuleUpdateFilterIndexContext( IndexContext delegate, IndexRule rule )
    {
        super( delegate );
        this.rule = rule;
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        super.update( new FilteringIterable<NodePropertyUpdate>( updates, ruleMatchingUpdates ) );
    }
}
