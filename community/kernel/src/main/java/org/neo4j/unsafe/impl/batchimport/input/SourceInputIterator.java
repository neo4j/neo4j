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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

/**
 * Used by {@link StoreMigrator} for providing {@link RelationshipRecord} and {@link NodeRecord}
 * data to {@link BatchImporter}.
 * @param <T> Type of items in this iterator
 * @param <U> Type of underlying item to convert from
 */
public abstract class SourceInputIterator<T,U>
        implements InputIterator<T>
{
    private final SourceTraceability source;

    public SourceInputIterator( SourceTraceability source )
    {
        this.source = source;
    }

    @Override
    public String sourceDescription()
    {
        return source.sourceDescription();
    }

    @Override
    public long lineNumber()
    {
        return source.lineNumber();
    }

    @Override
    public long position()
    {
        return source.position();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
