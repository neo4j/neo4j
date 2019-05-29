/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.extension.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class ActorsManager
{
    private final ConcurrentLinkedQueue<ActorImpl> actors = new ConcurrentLinkedQueue<>();

    private final String managerName;
    private final ThreadGroup threadGroup;

    ActorsManager( String name )
    {
        this.managerName = name;
        this.threadGroup = new ThreadGroup( managerName );
    }

    void stopAllActors() throws InterruptedException
    {
        List<ActorImpl> stoppedActors = new ArrayList<>();
        ActorImpl actor;
        while ( (actor = actors.poll()) != null )
        {
            actor.stop();
            stoppedActors.add( actor );
        }
        for ( ActorImpl stoppedActor : stoppedActors )
        {
            stoppedActor.join();
        }
    }

    Actor createActor( String name )
    {
        ActorImpl actor = new ActorImpl( threadGroup, "Actor:" + managerName + "." + name );
        actors.offer( actor );
        return actor;
    }
}
