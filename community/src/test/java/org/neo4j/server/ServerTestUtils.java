package org.neo4j.server;

import java.io.File;
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
    
    public static File createTempPropertyFile(File parentDir) throws IOException {
        File f = new File(parentDir, "test-" + new Random().nextInt() + ".properties");
        f.deleteOnExit();
        return f;
    }
}
