require 'vagrant/systems/linux'

module Neo4j
  class Cygwin < Vagrant::Systems::Linux
    def prepare_host_only_network(net_options=nil)
      # No-op
    end

    def enable_host_only_network(net_options)
      vm.ssh.execute do |ssh|
        # Windows interfaces start counting from 1,
        # so we need to increase vagrants interface 
        # number by one.
        iface_no = net_options[:adapter] + 1
        ssh.exec!("netsh interface ip set address 'Local Area Connection #{iface_no}' static address=#{net_options[:ip]} mask=#{net_options[:netmask]} store=persistent")
      end
    end
  end
end


