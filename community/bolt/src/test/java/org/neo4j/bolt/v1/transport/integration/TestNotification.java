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
package org.neo4j.bolt.v1.transport.integration;

import java.util.Map;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.SeverityLevel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

public class TestNotification implements Notification
{
    private final String code;
    private final String title;
    private final String description;
    private final SeverityLevel severityLevel;
    private final InputPosition position;

    public TestNotification( String code, String title, String description, SeverityLevel severityLevel,
            InputPosition position )
    {
        this.code = code;
        this.title = title;
        this.description = description;
        this.severityLevel = severityLevel;
        this.position = position != null ? position : InputPosition.empty;
    }

    @SuppressWarnings( "unchecked" )
    public static Notification fromMap( Map<String,Object> notification )
    {
        assertThat(notification, hasKey("code"));
        assertThat(notification, hasKey("title"));
        assertThat(notification, hasKey("description"));
        assertThat(notification, hasKey("severity"));
        InputPosition position = null;
        if ( notification.containsKey( "position" ) )
        {
            Map<String,Long> pos = (Map<String,Long>) notification.get( "position" );
            assertThat( pos, hasKey( "offset" ) );
            assertThat( pos, hasKey( "line" ) );
            assertThat( pos, hasKey( "column" ) );
            position = new InputPosition( pos.get("offset").intValue(), pos.get("line").intValue(), pos.get("column").intValue() );
        }

        return new TestNotification( (String) notification.get("code"),
                (String) notification.get("title"),
                (String) notification.get("description"),
                SeverityLevel.valueOf(  (String) notification.get( "severity")), position );
    }

    @Override
    public String getCode()
    {
        return code;
    }

    @Override
    public String getTitle()
    {
        return title;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public SeverityLevel getSeverity()
    {
        return severityLevel;
    }

    @Override
    public InputPosition getPosition()
    {
        return position;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TestNotification that = (TestNotification) o;

        if ( code != null ? !code.equals( that.code ) : that.code != null )
        {
            return false;
        }
        if ( title != null ? !title.equals( that.title ) : that.title != null )
        {
            return false;
        }
        if ( description != null ? !description.equals( that.description ) : that.description != null )
        {
            return false;
        }
        if ( severityLevel != that.severityLevel )
        {
            return false;
        }
        return position != null ? position.equals( that.position ) : that.position == null;

    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (severityLevel != null ? severityLevel.hashCode() : 0);
        result = 31 * result + (position != null ? position.hashCode() : 0);
        return result;
    }
}
