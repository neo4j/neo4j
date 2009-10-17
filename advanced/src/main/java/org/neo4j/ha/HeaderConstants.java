package org.neo4j.ha;

public class HeaderConstants
{
    static final byte SLAVE_GREETING = 0x01;
    static final byte MASTER_GREETING = 0x02;
    
    static final byte BYE = 0x10;

    static final byte REQUEST_LOG = 0x05;
    static final byte OFFER_LOG = 0x06;
    static final byte OK = 0x07;
    static final byte NOT_OK = 0x09;
}
