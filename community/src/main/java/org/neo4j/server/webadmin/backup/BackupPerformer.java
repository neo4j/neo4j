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

package org.neo4j.server.webadmin.backup;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.onlinebackup.Backup;
import org.neo4j.onlinebackup.Neo4jBackup;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.webadmin.domain.BackupFailedException;
import org.neo4j.server.webadmin.domain.NoBackupFoundationException;

public class BackupPerformer {

    public final static String LOGICAL_LOG_REGEX = "\\.v[0-9]+$";

    /**
     * Keep track of what directories are currently being used, to avoid
     * multiple backup jobs working in the same directory at once.
     */
    private static final Set<File> lockedPaths = Collections.synchronizedSet(new HashSet<File>());

    public static void doBackup(File backupPath) throws NoBackupFoundationException, BackupFailedException, DatabaseBlockedException {
        ensurePathIsLocked(backupPath);

        try {
            // Naive check to see if folder is initialized
            // I don't want to add an all-out check here, it'd be better
            // for the Neo4jBackup class to throw an exception.
            if (backupPath.listFiles() == null || backupPath.listFiles().length == 0 || !(new File(backupPath, "neostore")).exists()) {
                throw new NoBackupFoundationException("No foundation in: " + backupPath.getAbsolutePath());
            }

            // Perform backup
            GraphDatabaseService genericDb = NeoServer.server().database().db;

            if (genericDb instanceof EmbeddedGraphDatabase) {

                Backup backup = Neo4jBackup.allDataSources((EmbeddedGraphDatabase) genericDb, backupPath.getAbsolutePath());

                backup.doBackup();
            } else {
                throw new UnsupportedOperationException("Performing backups on non-local databases is currently not supported.");
            }
        } catch (IllegalStateException e) {
            throw new NoBackupFoundationException("No foundation in: " + backupPath.getAbsolutePath());
        } catch (IOException e) {
            throw new BackupFailedException("IOException while performing backup, see nested.", e);
        } finally {
            lockedPaths.remove(backupPath);
        }
    }

    public static void doBackupFoundation(File backupPath) throws BackupFailedException {
        ensurePathIsLocked(backupPath);

    }

    /**
     * Try to add a given file to the lockedPaths set.
     * 
     * @param path
     * @return true if path was added, false if path was already locked.
     */
    private static synchronized boolean lockPath(File path) {
        if (lockedPaths.contains(path)) {
            return false;
        } else {
            lockedPaths.add(path);
            return true;
        }
    }

    /**
     * Runs until it is able to lock a given path.
     * 
     * @param path
     */
    private static void ensurePathIsLocked(File path) {
        try {
            while (!lockPath(path)) {
                Thread.sleep(13);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
