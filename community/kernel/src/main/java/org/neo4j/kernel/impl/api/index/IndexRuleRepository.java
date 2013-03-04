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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

/**
 * Keeps track of index rules.
 */
public class IndexRuleRepository
{
    private final Map<Long, Set<Long>> indexedProperties = new HashMap<Long, Set<Long>>();

    public Iterator<Long> getIndexedProperties( long labelId )
    {
        return indexedProperties.containsKey( labelId ) ?
                indexedProperties.get( labelId ).iterator() :
                Collections.<Long>emptyList().iterator();
    }

    public void add( IndexRule rule )
    {
        if(!indexedProperties.containsKey( rule.getLabel() ))
        {
            indexedProperties.put( rule.getLabel(), new HashSet<Long>());
        }

        indexedProperties.get( rule.getLabel() ).add( rule.getPropertyKey() );
    }

    public void remove( IndexRule rule )
    {
        if(indexedProperties.containsKey( rule.getLabel() ))
        {
            indexedProperties.get( rule.getLabel() ).remove( rule.getPropertyKey() );
        }
    }
}
