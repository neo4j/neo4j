/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
