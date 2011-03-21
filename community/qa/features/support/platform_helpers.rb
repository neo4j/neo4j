module PlatformHelpers

  require 'rbconfig'


  ## TODO use better enum-pattern in ruby
  class Platform
    attr_reader :type, :extension

    def initialize(type, extension)
      @type = type
      @extension = extension
    end

    WINDOWS = Platform.new('windows', 'zip')
    UNIX = Platform.new('unix', 'tar.gz')
    UNKNOWN = Platform.new('unknown', nil)

    def supported?
      @extension != nil
    end

    def windows?
      @type == WINDOWS.type
    end

    def mac?
      @type == UNIX.type
    end

    def unknown?
      @type == UNKNOWN.type
    end
  end


  def current_platform
    platform = RbConfig::CONFIG['target_os']
    if platform =~ /win32/
      Platform::WINDOWS
    elsif platform =~ /linux/ || platform =~ /darwin/ || platform =~ /freebsd/
      Platform::UNIX
    else
      Platform::UNKNOWN
    end
  end

end
