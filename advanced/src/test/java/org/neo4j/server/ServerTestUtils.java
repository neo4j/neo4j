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

package org.neo4j.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class ServerTestUtils {
    public static File createTempDir() throws IOException {

        File d = File.createTempFile("neo4j-test", "dir");
        if (!d.delete())
            throw new RuntimeException("temp config directory pre-delete failed");
        if (!d.mkdirs())
            throw new RuntimeException("temp config directory not created");
        d.deleteOnExit();
        return d;
    }

    public static File createTempPropertyFile() throws IOException {
        return createTempPropertyFile(createTempDir());
    }

    public static void writePropertyToFile(String name, String value, File propertyFile) {
        try {
            FileWriter fstream = new FileWriter(propertyFile, true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(name);
            out.write("=");
            out.write(value);
            out.write(System.getProperty("line.separator"));
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File createTempPropertyFile(File parentDir) throws IOException {
        File f = new File(parentDir, "test-" + new Random().nextInt() + ".properties");
        f.deleteOnExit();
        return f;
    }
}
