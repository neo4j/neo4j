/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Controls authorization and authentication for a set of users.
 */
public class GroupRecord
{
    /*
      Design note: These instances are shared across threads doing disparate things with them, and there are no access
      locks. Correctness depends on write-time assertions and this class remaining immutable. Please do not introduce
      mutable fields here.
     */
    /** Group name */
    private final String name;

    /** Member users */
    private final SortedSet<String> users;

    public GroupRecord( String name, SortedSet<String> users )
    {
        this.name = name;
        this.users = users;
    }

    public GroupRecord( String name, String... users )
    {
        this.name = name;
        this.users = new TreeSet<String>();

        for( String user : users )
        {
            this.users.add( user );
        }
    }

    public String name()
    {
        return name;
    }

    public SortedSet<String> users() { return users; }

    /** Use this group as a base for a new group object */
    public Builder augment() { return new Builder(this); }

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

        GroupRecord group = (GroupRecord) o;

        if ( name != null ? !name.equals( group.name ) : group.name != null )
        {
            return false;
        }
        if ( users != null ? !users.equals( group.users ) : group.users != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + ( users != null ? users.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Group{" +
                "name='" + name + '\'' +
                ", users='" + users + '\'' +
                '}';
    }

    public static class Builder
    {
        private String name;
        private SortedSet<String> users;

        public Builder() { }

        public Builder( GroupRecord base )
        {
            name = base.name;
            users = base.users;
        }

        public Builder withName( String name ) { this.name = name; return this; }
        public Builder withUsers( SortedSet<String> users ) { this.users = users; return this; }
        public Builder withUser( String user ) { this.users.add( user ); return this; }

        public GroupRecord build()
        {
            return new GroupRecord( name, users );
        }
    }
}
