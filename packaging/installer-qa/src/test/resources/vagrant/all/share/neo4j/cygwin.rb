require 'vagrant/guest/linux'

module Neo4j
  class Cygwin < Vagrant::Guest::Linux

    def configure_networks(networks)
      networks.each do |network|
        vm.ssh.execute do |ssh|
          # Windows interfaces start counting from 1,
          # so we need to increase vagrants interface 
          # number by one.
          iface_no = network[:interface] + 1
          ssh.exec!("netsh interface ip set address 'Local Area Connection #{iface_no}' static address=#{network[:ip]} mask=#{network[:netmask]} store=persistent")
        end
      end
    end
  end
end


