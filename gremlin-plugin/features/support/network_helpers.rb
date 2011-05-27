module NetworkHelpers

  require 'socket'
  require 'timeout'

  def is_port_open?(ip, port)
    begin
      Timeout::timeout(1) do
        begin
          s = TCPSocket.new(ip, port)
          s.close
          return true
        rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
          return false
        end
      end
    rescue Timeout::Error
    end

    false
  end

  def copy_file(source, target)
    puts "copy '#{source}' -> '#{target}'"
    File.open(source, "r") do |src|
      open(target, "wb") do |file|
        while buf = src.read(2048)
          file.write(buf)
        end
      end
    end
  end

  def transfer_if_newer(location, target)
    puts "transfer_if_newer(#{location},#{target})"

    if (location.scheme == "http") then
      server = Net::HTTP.new(location.host, 80)
      head = server.head(location.path)
      server_time = Time.httpdate(head['last-modified'])
      if (!File.exists?(target) || server_time != File.mtime(target))
        puts target+" missing or newer version as on #{location} - downloading"
        server.request_get(location.path) do |res|
          open(target, "wb") do |file|
            res.read_body do |segment|
              file.write(segment)
            end
          end
        end
        File.utime(0, server_time, target)
      else
        puts target+" not modified - download skipped"
      end
    elsif (location.scheme == "file") then
      copy_file(location.path, target)
    else
      raise 'unsupported schema ' + location
    end
  end

  def fix_file_sep(file)
    file.tr('/', '\\')
  end

  def unzip(full_archive_name, target)
    if (current_platform.unix?)
      exec_wait("unzip -o #{full_archive_name} -d #{target}")
    elsif (current_platform.windows?)
      exec_wait("cmd /c " + fix_file_sep(File.expand_path("../../support/unzip.vbs", __FILE__)) + " " +
                    fix_file_sep(full_archive_name) + " " + target)
    else
      raise 'platform not supported'
    end
  end

  def exec_wait(cmd)
    puts "exec: '#{cmd}'"
    puts `#{cmd}`
    raise "execution failed(#{$?})" unless $?.to_i == 0
  end

  def untar(archive, target)
    if (current_platform.unix?)
      pushd target
      exec_wait("tar xzf #{archive} --strip-components 1")
      popd
    else
      raise 'platform not supported'
    end
  end
end

