package org.neo4j.server.rest.web.security;

public class SslConfiguration {

    private final String keyStorePath;
    private final String keyStorePassword;
    private final String keyPassword;
    
    public SslConfiguration(String keyStorePath, String keyStorePassword, String keyPassword) {
        this.keyStorePassword = keyStorePassword;
        this.keyStorePath = keyStorePath;
        this.keyPassword = keyPassword;
    }
    
    public String getKeyStorePath() {
        return keyStorePath;
    }
    
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }
    
}
