module ShellHelpers

  require 'fileutils'

  @@dir_stack = []
  
  def init_dir(dir_path)
    FileUtils.rm_rf(dir_path)
    Dir.mkdir(dir_path) if !File.directory?(dir_path)
  end

  def mkdir(dir_path)
    Dir.mkdir(dir_path) if !File.directory?(dir_path)
  end

  def working_dir
    @working_dir
  end

  def working_dir=(dir_path)
    @working_dir = dir_path
  end
  
  # not a very robust check for compatibility
  def bash_compatible
    shell = `sh --version`
    return ($?.to_i == 0)
  end

  def setenv(varname, value)
    ENV["#{varname}"] = "#{value}"
  end

  def getenv(varname)
    bash("echo $#{varname}")
    
    return @stdout
  end

  def test_executable(executable_path)
    bash("[[ -x #{executable_path} ]]")
    return (@exit_code == 0)
  end

  def test_directory(dir_path)
    bash("[[ -d #{dir_path} ]]")
    return (@exit_code == 0)
  end

  def test_file(dir_path)
    bash("[[ -f #{dir_path} ]]")
    return (@exit_code == 0)
  end

  def bash(cmdline, verbose = false)
    puts "$ " + cmdline if (verbose)
    @stdout = `#{cmdline}`
    @exit_code = $?.to_i    
  end

  def last_stdout
    return @stdout
  end

  def last_exit_code
    return @exit_code
  end

  def create_file(file_name, contents)
    file_path = File.join(file_name)
    File.open(file_path, "w") { |f| f << contents }
  end
  
  def file_contains(file_name, pattern)
    r = Regexp.new(pattern)
    File.open( file_name ).any? {|line|
      r =~ line
    }
  end

  def pushd(path)
    @@dir_stack.push(Dir.getwd)
    Dir.chdir(path)
  end

  def popd
    Dir.chdir(@@dir_stack.pop) if !@@dir_stack.empty?
  end

end


