package org.neo4j.server.rest;

import java.io.File;

import org.neo4j.server.NeoServer;

public abstract class TestUtil {
    public static void deleteTestDb() {
        deleteFileOrDirectory(new File(NeoServer.server().database().getLocation()));
    }

    public static void deleteFileOrDirectory(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteFileOrDirectory(child);
            }
        } else {
            file.delete();
        }
    }
}
