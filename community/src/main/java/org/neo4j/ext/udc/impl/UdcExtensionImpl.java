package org.neo4j.ext.udc.impl;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;

/**
 * Kernel extension for UDC.
 */
@Service.Implementation( KernelExtension.class )
public class UdcExtensionImpl extends KernelExtension {

  public UdcExtensionImpl() {
    super("kernel udc");
  }

  @Override
  protected void load(KernelData kernel) {
    System.err.println("UDC extension loaded!");
  }
}
