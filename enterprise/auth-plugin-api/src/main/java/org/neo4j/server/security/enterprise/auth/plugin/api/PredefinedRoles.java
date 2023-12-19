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
package org.neo4j.server.security.enterprise.auth.plugin.api;

/**
 * The role names of the built-in predefined roles of Neo4j.
 */
public class PredefinedRoles
{
    public static final String ADMIN = "admin";
    public static final String ARCHITECT = "architect";
    public static final String PUBLISHER = "publisher";
    public static final String EDITOR = "editor";
    public static final String READER = "reader";

    private PredefinedRoles()
    {
    }
}
