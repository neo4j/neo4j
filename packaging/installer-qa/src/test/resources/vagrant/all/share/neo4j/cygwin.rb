require 'vagrant/guest/linux'

module Neo4j
  class Cygwin < Vagrant::Guest::Linux

    def configure_networks(networks)
      networks.each do |network|
        # Windows interfaces start counting from 1,
        # so we need to increase vagrants interface 
        # number by one.
        iface_no = network[:interface] + 1
        vm.channel.sudo("netsh interface ip set address 'Local Area Connection #{iface_no}' static address=#{network[:ip]} mask=#{network[:netmask]} store=persistent")
      end
    end

    def halt
      @vm.channel.sudo("halt")

      # Wait until the VM's state is actually powered off. If this doesn't
      # occur within a reasonable amount of time (15 seconds by default),
      # then simply return and allow Vagrant to kill the machine.
      count = 0
      while @vm.state != :poweroff
        count += 1

        return if count >= @vm.config.linux.halt_timeout
        sleep @vm.config.linux.halt_check_interval
      end
    end

    def mount_shared_folder(name, guestpath, options)
        real_guestpath = expanded_guest_path(guestpath)
        #@logger.debug("Shell expanded guest path: #{real_guestpath}")

        #@vm.channel.sudo("mkdir -p #{real_guestpath}")
        #mount_folder(name, real_guestpath, options)
        #@vm.channel.sudo("chown `id -u #{options[:owner]}`:`id -g #{options[:group]}` #{real_guestpath}")
      end

    def mount_nfs(ip, folders)
      # TODO: Maybe check for nfs support on the guest, since its often
      # not installed by default
      folders.each do |name, opts|
        # Expand the guestpath, so we can handle things like "~/vagrant"
        real_guestpath = expanded_guest_path(opts[:guestpath])

        # TODO: Implement (below is for linux, for reference)
        #@vm.channel.sudo("mkdir -p #{real_guestpath}")
        #@vm.channel.sudo("mount -o vers=#{opts[:nfs_version]} #{ip}:'#{opts[:hostpath]}' #{real_guestpath}",
        #                :error_class => LinuxError,
        #                :error_key => :mount_nfs_fail)
      end
    end
  end
end


