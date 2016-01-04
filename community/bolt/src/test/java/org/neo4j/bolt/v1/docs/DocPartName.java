/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.docs;

import static java.lang.String.format;

public final class DocPartName
{
    private final String fileName;
    private final String title;
    private final String detail;

    public static DocPartName create( String fileName, String title )
    {
        return new DocPartName( fileName, title, null );
    }

    public static DocPartName unknown()
    {
        return DocPartName.create( "?", "?" );
    }

    private DocPartName( String fileName, String title, String detail )
    {
        this.fileName = fileName;
        this.title = title;
        this.detail = detail;
    }

    public DocPartName withDetail( String detail )
    {
        return new DocPartName( fileName, title, detail );
    }

    @Override
    public String toString()
    {
        if ( detail == null )
        {
            return format( "%s[%s]", fileName, title );
        }
        else
        {
            return format( "%s[%s]: %s", fileName, title, detail );
        }
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

        DocPartName that = (DocPartName) o;

        return fileName.equals( that.fileName )
                && title.equals( that.title )
                && !(detail != null ? !detail.equals( that.detail ) : that.detail != null);

    }

    @Override
    public int hashCode()
    {
        int result = fileName.hashCode();
        result = 31 * result + title.hashCode();
        result = 31 * result + (detail != null ? detail.hashCode() : 0);
        return result;
    }
}
