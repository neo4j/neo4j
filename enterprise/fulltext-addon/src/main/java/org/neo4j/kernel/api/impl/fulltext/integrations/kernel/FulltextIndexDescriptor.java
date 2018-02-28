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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

public class FulltextIndexDescriptor extends IndexDescriptor
{
    private final Set<String> propertyNames;
    private final String identifier;
    private final String analyzer;

    public FulltextIndexDescriptor( SchemaDescriptor schema, String name, PropertyKeyTokenHolder propertyKeyTokenHolder, String analyzer )
            throws TokenNotFoundException
    {
        super( schema, Type.NON_SCHEMA );
        this.analyzer = analyzer;
        propertyNames = new HashSet<>();
        this.identifier = name;
        for ( int propertyId : schema.getPropertyIds() )
        {
            propertyNames.add( propertyKeyTokenHolder.getTokenById( propertyId ).name() );
        }
    }

    @Override
    public String metadata()
    {
        return analyzer();
    }

    @Override
    public String identifier()
    {
        return identifier;
    }

    public Collection<String> propertyNames()
    {
        return Collections.unmodifiableSet( propertyNames );
    }

    public String analyzer()
    {
        return analyzer;
    }
}
