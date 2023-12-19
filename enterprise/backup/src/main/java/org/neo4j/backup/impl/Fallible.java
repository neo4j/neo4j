/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Contains a reference to a class (designed for enums) and can optionally also contain a throwable if the provided state has an exception attached
 *
 * @param <T> generic of an enum (not enforced)
 */
class Fallible<T>
{
    private final T state;
    private final Throwable cause;

    public Optional<Throwable> getCause()
    {
        return Optional.ofNullable( cause );
    }

    public T getState()
    {
        return state;
    }

    Fallible( T state, @Nullable Throwable cause )
    {
        this.state = state;
        this.cause = cause;
    }
}
