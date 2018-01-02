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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface State
{
    Strategy value();

    enum Strategy implements ActiveState.Factory
    {
        CONCURRENT_HASH_MAP
        {
            @Override
            public <Key> ActiveState<Key> open( ReadableState<Key> store, File file )
            {
                return new ConcurrentMapState<>( store, file );
            }
        },
        READ_ONLY_CONCURRENT_HASH_MAP
        {
            @Override
            public <Key> ActiveState<Key> open( ReadableState<Key> store, File file )
            {
                return new ConcurrentMapState<Key>( store, file ) {
                    @Override
                    protected boolean hasChanges()
                    {
                        return false;
                    }
                };
            }
        }
    }
}
