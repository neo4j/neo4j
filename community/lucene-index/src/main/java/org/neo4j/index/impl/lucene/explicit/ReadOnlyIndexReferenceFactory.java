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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;

public class ReadOnlyIndexReferenceFactory extends IndexReferenceFactory
{
    public ReadOnlyIndexReferenceFactory( LuceneDataSource.LuceneFilesystemFacade filesystemFacade, File baseStorePath,
            IndexTypeCache typeCache )
    {
        super( filesystemFacade, baseStorePath, typeCache );
    }

    @Override
    IndexReference createIndexReference( IndexIdentifier identifier )
            throws IOException, ExplicitIndexNotFoundKernelException
    {
        IndexReader reader = DirectoryReader.open( getIndexDirectory( identifier ) );
        IndexSearcher indexSearcher = newIndexSearcher( identifier, reader );
        return new ReadOnlyIndexReference( identifier, indexSearcher );
    }

    @Override
    IndexReference refresh( IndexReference indexReference )
    {
        return indexReference;
    }
}
