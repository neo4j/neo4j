package org.neo4j.dbms.systemgraph;

import java.time.Duration;

import org.neo4j.logging.Level;

public final class DriverSettings
{
    enum Keys
    {
        SSL_ENABLED,
        CONNECTION_TIMEOUT,
        CONNECTION_MAX_LIFETIME,
        CONNECTION_POOL_ACQUISITION_TIMEOUT,
        CONNECTION_POOL_IDLE_TEST,
        CONNECTION_POOL_MAX_SIZE,
        LOGGING_LEVEL;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private final boolean sslEnabled;
    private final Duration connectionTimeout;
    private final Duration connectionMaxLifetime;
    private final Duration connectionPoolAcquisitionTimeout;
    private final Duration connectionPoolIdleTest;
    private final int connectionPoolMaxSize;
    private final Level loggingLevel;

    DriverSettings( boolean sslEnabled, Duration connectionTimeout, Duration connectionMaxLifetime, Duration connectionPoolAcquisitionTimeout,
            Duration connectionPoolIdleTest, int connectionPoolMaxSize, Level loggingLevel )
    {
        this.sslEnabled = sslEnabled;
        this.connectionTimeout = connectionTimeout;
        this.connectionMaxLifetime = connectionMaxLifetime;
        this.connectionPoolAcquisitionTimeout = connectionPoolAcquisitionTimeout;
        this.connectionPoolIdleTest = connectionPoolIdleTest;
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        this.loggingLevel = loggingLevel;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public Duration connectionTimeout()
    {
        return connectionTimeout;
    }

    public Duration connectionMaxLifetime()
    {
        return connectionMaxLifetime;
    }

    public Duration connectionPoolAcquisitionTimeout()
    {
        return connectionPoolAcquisitionTimeout;
    }

    public Duration connectionPoolIdleTest()
    {
        return connectionPoolIdleTest;
    }

    public int connectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    public Level loggingLevel()
    {
        return loggingLevel;
    }
}
