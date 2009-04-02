package org.neo4j.onlinebackup;

import java.io.IOException;

/**
 * Online backup for neo4j.
 */
public interface Backup
{
    /**
     * Perform the backup.
     */
    void doBackup() throws IOException;

    /**
     * Enable logging to file. The log messages will be appended to the
     * backup.log file in the current working directory.
     */
    void enableFileLogger() throws SecurityException, IOException;

    /**
     * Disable logging to file.
     */
    void disableFileLogger();

    /**
     * Enable debug logging. Adds debug output to both console and file (if file
     * output is enabled).
     */
    void setLogLevelDebug();

    /**
     * Set logging to normal. Changes settings for both console and file output.
     */
    void setLogLevelNormal();

    /**
     * Turn off all logging.
     */
    void setLogLevelOff();
}