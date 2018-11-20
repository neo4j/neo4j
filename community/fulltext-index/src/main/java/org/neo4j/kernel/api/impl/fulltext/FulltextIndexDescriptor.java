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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

class FulltextIndexDescriptor implements StorageIndexReference
{
    private final StorageIndexReference descriptor;
    private final String[] propertyNames;
    private final Analyzer analyzer;
    private final String analyzerName;
    private final boolean eventuallyConsistent;

    FulltextIndexDescriptor( StorageIndexReference descriptor, String[] propertyNames, Analyzer analyzer, String analyzerName, boolean eventuallyConsistent )
    {
        this.descriptor = descriptor;
        this.propertyNames = propertyNames;
        this.analyzer = analyzer;
        this.analyzerName = analyzerName;
        this.eventuallyConsistent = eventuallyConsistent;
    }

    String[] propertyNames()
    {
        return propertyNames;
    }

    public Analyzer analyzer()
    {
        return analyzer;
    }

    String analyzerName()
    {
        return analyzerName;
    }

    @Override
    public boolean isEventuallyConsistent()
    {
        return eventuallyConsistent;
    }

    @Override
    public boolean isUnique()
    {
        return descriptor.isUnique();
    }

    @Override
    public String name()
    {
        return descriptor.name();
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return descriptor.userDescription( tokenNameLookup );
    }

    @Override
    public boolean isFulltextIndex()
    {
        return true;
    }

    @Override
    public long indexReference()
    {
        return descriptor.indexReference();
    }

    @Override
    public SchemaDescriptor schema()
    {
        return descriptor.schema();
    }
}
