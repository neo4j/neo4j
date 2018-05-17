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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;

public class FulltextIndexDescriptor extends StoreIndexDescriptor
{
    private final Set<String> propertyNames;
    private final String analyzer;

    @Override
    public IndexDescriptor.Type type()
    {
        return IndexDescriptor.Type.MULTI_TOKEN;
    }

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor, PropertyKeyTokenHolder propertyKeyTokenHolder, String analyzer )
    {
        super( descriptor );
        this.analyzer = analyzer;
        Set<String> names = new HashSet<>();
        for ( int propertyId : schema.getPropertyIds() )
        {
            names.add( propertyKeyTokenHolder.getTokenByIdOrNull( propertyId ).name() );
        }
        propertyNames = Collections.unmodifiableSet( names );
    }

//    @Override
//    public String metadata()
//    {
//        return analyzer();
//    }

    public Collection<String> propertyNames()
    {
        return propertyNames;
    }

    public String analyzer()
    {
        return analyzer;
    }
}
