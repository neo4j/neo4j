# Bolt Traffic Analyzer

This is a tool that can analyze network traffic captured in `pcap` format, and be used to understand the timing
involved in the exchanges. 

By using a post-analysis and a third-party measuring point (eg. `tcpdump`), we get a view of who is driving latency
that is measured independently of either the server or the client. We also get a detailed view synthesised from the 
raw network data, meaning the tool can analyze things like packet utilization.

## Capturing network traffic

TL;DR:

    # List available interfaces
    tcpdump -D
    
    # Capture all traffic going to the Bolt port on your chosen interface,
    # and save it to `capture.pcap`
    tcpdump -s 0 -w capture.pcap -i <INTERFACE> port 7687
    
More info: https://danielmiessler.com/study/tcpdump/

## Analyzing the traffic

Once you have a traffic dump, simply provide it to boltalyzer:

    boltalyzer capture.pcap
    
More info:

    boltalyzer -h
    

## Understanding the output

The capture file is converted to a stream of 

    <time since last message in session, usec> <session name> <actor> <packet payload>
    
Where packet payload is the messages that were completed in the given packet - notably, they may have been 
started in a prior packet!

    00000003 session-000 Client <HANDSHAKE>
    00000003 session-000 Server <EMPTY>
    00000012 session-000 Server <HANDSHAKE RESPONSE>
    00000013 session-000 Client <EMPTY>
    00000034 session-000 Client
      INIT
      RUN CREATE (n {name:{name}}) RETURN id(n)
      PULL_ALL
    00000034 session-000 Server <EMPTY>
    00000244 session-000 Server
      SUCCESS
      SUCCESS
      RECORD
      SUCCESS
      
