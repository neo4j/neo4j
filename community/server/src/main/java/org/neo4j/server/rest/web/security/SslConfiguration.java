package org.neo4j.server.rest.web.security;

public class SslConfiguration {

    private final String keyStorePath;
    private final String keyStorePassword;
    
    public SslConfiguration(String keyStorePath, String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        this.keyStorePath = keyStorePath;
    }
    
    public String getKeyStorePath() {
        return keyStorePath;
    }
    
    public String getKeyStorePassword() {
        return keyStorePassword;
    }
    
}
