module PlatformHelpers

  require 'rbconfig'

  class Platform
    attr_reader :type, :extension

    def initialize(type, extension)
      @type = type
      @extension = extension
    end

    def supported?
      @extension != nil
    end

    def windows?
      @type == "windows"
    end

    def unix?
      @type == "unix"
    end

    def unknown?
      @type == "unknown"
    end
  end

  def current_platform
    platform = RbConfig::CONFIG['target_os']
    if platform =~ /win32/
      Platform.new('windows', 'zip')
    elsif platform =~ /linux/ || platform =~ /darwin/ || platform =~ /freebsd/
      Platform.new('unix', 'tar.gz')
    else
      Platform.new('unknown', nil)
    end
  end

end
