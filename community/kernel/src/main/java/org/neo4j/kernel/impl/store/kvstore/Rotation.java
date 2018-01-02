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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Rotation
{
    Strategy value();

    String[] parameters() default {".a", ".b"};

    enum Strategy
    {
        LEFT_RIGHT
        {
            @Override
            RotationStrategy create( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                                     RotationMonitor monitor, File base,
                                     String[] parameters )
            {
                if ( parameters == null || parameters.length != 2 )
                {
                    throw new IllegalArgumentException( "Expected exactly 2 format parameters." );
                }
                String parent = base.getParent();
                String l = base.getName() + parameters[0], r = base.getName() + parameters[1];
                final File left = new File( parent, l ), right = new File( parent, r );
                return new RotationStrategy.LeftRight( fs, pages, format, monitor, left, right );
            }
        },
        INCREMENTING
        {
            @Override
            RotationStrategy create( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                                     RotationMonitor monitor, File base,
                                     String[] parameters )
            {
                return new RotationStrategy.Incrementing( fs, pages, format, monitor, base );
            }
        };

        abstract RotationStrategy create( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                                          RotationMonitor monitor, File base, String... parameters );
    }
}
