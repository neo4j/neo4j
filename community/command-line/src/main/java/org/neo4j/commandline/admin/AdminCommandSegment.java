/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.admin;

public abstract class AdminCommandSegment
{
    private static final AdminCommandSegment GENERAL = new GeneralSegment();
    private static final AdminCommandSegment MANAGE_ONLINE_BACKUP = new ManageOnlineBackup();
    private static final AdminCommandSegment MANAGE_OFFLINE_BACKUP = new ManageOffineBackup();
    private static final AdminCommandSegment CLUSTERING = new Clustering();

    public abstract String printable();

    public static AdminCommandSegment general()
    {
        return GENERAL;
    }

    public static AdminCommandSegment manageOffineBackup()
    {
        return MANAGE_OFFLINE_BACKUP;
    }

    public static AdminCommandSegment clustering()
    {
        return CLUSTERING;
    }

    public static AdminCommandSegment manageOnlineBackup()
    {
        return MANAGE_ONLINE_BACKUP;
    }

    static class GeneralSegment extends AdminCommandSegment
    {
        @Override
        public String printable()
        {
            return "General";
        }
    }

    static class ManageOnlineBackup extends AdminCommandSegment
    {
        @Override
        public String printable()
        {
            return "Manage online backup";
        }

    }

    static class ManageOffineBackup extends AdminCommandSegment
    {
        @Override
        public String printable()
        {
            return "Manage offline backup";
        }
    }

    static class Clustering extends AdminCommandSegment
    {
        @Override
        public String printable()
        {
            return "Clustering";
        }
    }
}
