/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;

abstract class ProgressiveFormat<Meta> extends KeyValueStoreFileFormat<Meta>
{
    ProgressiveFormat( int maxSize, HeaderField<Meta, ?>... headerFields )
    {
        super( maxSize, headerFields );
    }

    public abstract int compareMetadata( Meta lhs, Meta rhs );

    public abstract Meta initialMetadata();

    public abstract int keySize();

    public abstract int valueSize();

    public abstract void failedToOpenStoreFile( File path, Exception error );

    public abstract void beforeRotation( File source, File target, Meta meta );

    public abstract void rotationSucceeded( File source, File target, Meta meta );

    public abstract void rotationFailed( File source, File target, Meta meta, Exception e );
}
