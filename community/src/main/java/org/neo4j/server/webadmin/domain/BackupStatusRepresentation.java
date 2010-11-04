/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.domain;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.rest.domain.Representation;

/**
 * Represents current status of the backup sub system.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class BackupStatusRepresentation implements Representation
{
    public enum CurrentAction
    {
        IDLE,
        BACKING_UP,
        CREATING_FOUNDATION,
        WAITING_FOR_FOUNDATION
    }

    protected CurrentAction currentAction;
    protected Date started;
    protected Date eta;

    public BackupStatusRepresentation()
    {
        this( CurrentAction.IDLE, null, null );
    }

    public BackupStatusRepresentation( CurrentAction currentAction,
            Date started, Date eta )
    {
        this.currentAction = currentAction;
        this.started = started;
        this.eta = eta;
    }

    public Object serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "current_action", currentAction );
        map.put( "started", started != null ? started.getTime() : 0 );
        map.put( "eta", eta != null ? eta.getTime() : 0 );
        return map;
    }

}
