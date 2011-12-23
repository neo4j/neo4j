require 'vagrant/systems/linux'

module Neo4j
  module Vagrant
    class Cygwin < Vagrant::Systems::Linux
      def prepare_host_only_network(net_options=nil)
        # No-op
      end

      def enable_host_only_network(net_options)
        vm.ssh.execute do |ssh|
          ssh.exec!("netsh interface ip set address 'Local Area Connection #{net_options[:adapter]}' static address=#{net_options[:ip]} mask=#{net_options[:netmask]} store=persistent")
        end
      end
    end
  end
end


