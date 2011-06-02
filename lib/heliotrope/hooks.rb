module Heliotrope

## largely cribbed from sup
class Hooks
  class Env
    def initialize name
      @__name = name
      @__cache = {}
    end

    def __run __hook, __filename, __locals
      __binding = binding
      __lprocs, __lvars = __locals.partition { |k, v| v.is_a?(Proc) }
      eval __lvars.map { |k, v| "#{k} = __locals[#{k.inspect}];" }.join, __binding
      ## we also support closures for delays evaluation. unfortunately
      ## we have to do this via method calls, so you don't get all the
      ## semantics of a regular variable. not ideal.
      __lprocs.each do |k, v|
        self.class.instance_eval do
          define_method k do
            @__cache[k] ||= v.call
          end
        end
      end
      ret = eval __hook, __binding, __filename
      @__cache = {}
      ret
    end
  end

  def initialize dir
    @dir = dir
    @hooks = {}
    @envs = {}

    Dir.mkdir dir unless File.exists? dir
  end

  def run name, locals={}
    hook = hook_for(name) or return
    env = @envs[hook] ||= Env.new(name)

    result = nil
    fn = fn_for name
    begin
      result = env.__run hook, fn, locals
    rescue Exception => e
      $stderr.puts "error running #{fn}: #{e.message}"
      $stderr.puts e.backtrace.join("\n")
      @hooks[name] = nil # disable it
    end
    result
  end

  def enabled? name; hook_for name end

private

  def hook_for name
    @hooks[name] ||= begin
      IO.read fn_for(name)
    rescue SystemCallError => e
      $stderr.puts "can't read hook: #{e.message}"
      nil
    end
  end

  def fn_for name
    File.join @dir, "#{name}.rb"
  end
end

end
