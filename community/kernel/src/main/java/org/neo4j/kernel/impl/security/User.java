/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.security;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Controls authorization and authentication for an individual user.
 */
public class User {
    /*
     Design note: These instances are shared across threads doing disparate things with them, and there are no access
     locks. Correctness depends on write-time assertions and this class remaining immutable. Please do not introduce
     mutable fields here.
    */
    /**
     * User name
     */
    private final String name;

    private final String id;

    /**
     * Authentication credentials used by the built in username/password authentication scheme
     */
    private final Credential credential;

    /**
     * Set of flags, eg. password_change_required
     */
    private final SortedSet<String> flags;

    public static final String PASSWORD_CHANGE_REQUIRED = "password_change_required";

    private User(String name, String id, Credential credential, SortedSet<String> flags) {
        this.name = name;
        this.id = id;
        this.credential = credential;
        this.flags = flags;
    }

    public String name() {
        return name;
    }

    /**
     * User ids were introduced in Neo4j 4.3-drop04.
     * The id is added when the user is added to the system database or on a system database upgrade from a previous version.
     * Thus, this method returning null indicates that
     * 1. the user was created before Neo4j 4.3-drop04 and has not been properly upgraded
     * or
     * 2. the user was created through a neo4j-admin command and has not yet been added to the system database.
     *
     * @return the user's id.
     */
    public String id() {
        return id;
    }

    public Credential credentials() {
        return credential;
    }

    public boolean hasFlag(String flag) {
        return flags.contains(flag);
    }

    public Iterable<String> getFlags() {
        return flags;
    }

    public boolean passwordChangeRequired() {
        return flags.contains(PASSWORD_CHANGE_REQUIRED);
    }

    /**
     * Use this user as a base for a new user object
     */
    public Builder augment() {
        return new Builder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        User user = (User) o;

        if (!flags.equals(user.flags)) {
            return false;
        }
        if (id != null ? !id.equals(user.id) : user.id != null) {
            return false;
        }
        if (credential != null ? !credential.equals(user.credential) : user.credential != null) {
            return false;
        }
        return name != null ? name.equals(user.name) : user.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (credential != null ? credential.hashCode() : 0);
        result = 31 * result + (flags.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "User{" + "name='"
                + name + '\'' + ", id='"
                + id + '\'' + ", credentials="
                + credential + ", flags="
                + flags + '}';
    }

    public static class Builder {
        private final String name;
        private String id;
        private Credential credential;
        private final SortedSet<String> flags = new TreeSet<>();

        public Builder(String name, Credential credential) {
            this.name = name;
            this.credential = credential;
        }

        public Builder(User base) {
            name = base.name;
            id = base.id;
            credential = base.credential;
            flags.addAll(base.flags);
        }

        public Builder withCredentials(Credential creds) {
            this.credential = creds;
            return this;
        }

        public Builder withFlag(String flag) {
            this.flags.add(flag);
            return this;
        }

        public Builder withoutFlag(String flag) {
            this.flags.remove(flag);
            return this;
        }

        public Builder withRequiredPasswordChange(boolean change) {
            if (change) {
                withFlag(PASSWORD_CHANGE_REQUIRED);
            } else {
                withoutFlag(PASSWORD_CHANGE_REQUIRED);
            }
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public User build() {
            return new User(name, id, credential, flags);
        }
    }
}
