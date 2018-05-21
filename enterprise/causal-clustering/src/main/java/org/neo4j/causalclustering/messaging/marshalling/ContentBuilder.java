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
package org.neo4j.causalclustering.messaging.marshalling;

import java.util.function.Function;

public class ContentBuilder<CONTENT>
{
    private boolean isComplete;
    private Function<CONTENT,CONTENT> contentFunction;

    public static <C> ContentBuilder<C> emptyUnfinished()
    {
        return new ContentBuilder<>( content -> content, false );
    }

    public static <C> ContentBuilder<C> unfinished( Function<C,C> contentFunction )
    {
        return new ContentBuilder<>( contentFunction, false );
    }

    public static <C> ContentBuilder<C> finished( C content )
    {
        return new ContentBuilder<>( c1 -> content, true );
    }

    private ContentBuilder( Function<CONTENT,CONTENT> contentFunction, boolean isComplete )
    {
        this.contentFunction = contentFunction;
        this.isComplete = isComplete;
    }

    public boolean isComplete()
    {
        return isComplete;
    }

    public ContentBuilder<CONTENT> combine( ContentBuilder<CONTENT> replicatedContentBuilder )
    {
        if ( isComplete )
        {
            throw new IllegalStateException( "This content builder has already completed and cannot be combined." );
        }
        contentFunction = contentFunction.compose( replicatedContentBuilder.contentFunction );
        isComplete = replicatedContentBuilder.isComplete;
        return this;
    }

    public CONTENT build()
    {
        if ( !isComplete )
        {
            throw new IllegalStateException( "Cannot build unfinished content" );
        }
        return contentFunction.apply( null );
    }
}
