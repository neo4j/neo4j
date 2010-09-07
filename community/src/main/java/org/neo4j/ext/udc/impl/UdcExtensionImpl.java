package org.neo4j.ext.udc.impl;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

import java.util.Timer;

/**
 * Kernel extension for UDC.
 */
@Service.Implementation( KernelExtension.class )
public class UdcExtensionImpl extends KernelExtension {
    private static final int FIRST_DELAY = 10;   //minutes
    private static final int INTERVAL = 10;      //minutes

    public UdcExtensionImpl() {
    super("kernel udc");
  }

  @Override
  protected void load(KernelData kernel) {
      Timer timer = new Timer();
      NeoStoreXaDataSource ds = (NeoStoreXaDataSource) kernel.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( "nioneodb" );
      String storeId = Long.toHexString( ds.getRandomIdentifier() );
      UdcTimerTask task = new UdcTimerTask("127.0.0.1", kernel.version(), storeId);
      timer.scheduleAtFixedRate(task, FIRST_DELAY * 1000 * 60, INTERVAL * 1000 * 60);
  }
}
