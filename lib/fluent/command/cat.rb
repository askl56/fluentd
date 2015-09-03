#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'optparse'
require 'fluent/env'

op = OptionParser.new

op.banner += ' <tag>'

port = Fluent::DEFAULT_LISTEN_PORT
host = '127.0.0.1'
unix = false
socket_path = Fluent::DEFAULT_SOCKET_PATH

config_path = Fluent::DEFAULT_CONFIG_PATH
format = 'json'
message_key = 'message'

op.on('-p', '--port PORT', "fluent tcp port (default: #{port})", Integer) do|i|
  port = i
end

op.on('-h', '--host HOST', "fluent host (default: #{host})") do|s|
  host = s
end

op.on('-u', '--unix', 'use unix socket instead of tcp', TrueClass) do|b|
  unix = b
end

op.on('-s', '--socket PATH', "unix socket path (default: #{socket_path})") do|s|
  socket_path = s
end

op.on('-f', '--format FORMAT', "input format (default: #{format})") do|s|
  format = s
end

op.on('--json', 'same as: -f json', TrueClass) do|_b|
  format = 'json'
end

op.on('--msgpack', 'same as: -f msgpack', TrueClass) do|_b|
  format = 'msgpack'
end

op.on('--none', 'same as: -f none', TrueClass) do|_b|
  format = 'none'
end

op.on('--message-key KEY', "key field for none format (default: #{message_key})") do|s|
  message_key = s
end

(class<<self; self; end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end

begin
  op.parse!(ARGV)

  usage nil if ARGV.length != 1

  tag = ARGV.shift

rescue
  usage $ERROR_INFO.to_s
end

require 'thread'
require 'monitor'
require 'socket'
require 'yajl'
require 'msgpack'

class Writer
  include MonitorMixin

  class TimerThread
    def initialize(writer)
      @writer = writer
    end

    def start
      @finish = false
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finish = true
      @thread.join
    end

    def run
      until @finish
        sleep 1
        @writer.on_timer
      end
    end
  end

  def initialize(tag, connector)
    @tag = tag
    @connector = connector
    @socket = false

    @socket_time = Time.now.to_i
    @socket_ttl = 10 # TODO
    @error_history = []

    @pending = []
    @pending_limit = 1024 # TODO
    @retry_wait = 1
    @retry_limit = 5 # TODO

    super()
  end

  def write(record)
    if record.class != Hash
      fail ArgumentError, "Input must be a map (got #{record.class})"
    end

    entry = [Time.now.to_i, record]
    synchronize do
      unless write_impl([entry])
        # write failed
        @pending.push(entry)

        while @pending.size > @pending_limit
          # exceeds pending limit; trash oldest record
          time, record = @pending.shift
          abort_message(time, record)
        end
      end
    end
  end

  def on_timer
    now = Time.now.to_i

    synchronize do
      unless @pending.empty?
        # flush pending records
        if write_impl(@pending)
          # write succeeded
          @pending.clear
        end
      end

      if @socket && @socket_time + @socket_ttl < now
        # socket is not used @socket_ttl seconds
        close
      end
    end
  end

  def close
    @socket.close
    @socket = nil
  end

  def start
    @timer = TimerThread.new(self)
    @timer.start
    self
  end

  def shutdown
    @timer.shutdown
  end

  private

  def write_impl(array)
    socket = get_socket
    return false unless socket

    begin
      socket.write [@tag, array].to_msgpack
      socket.flush
    rescue
      $stderr.puts "write failed: #{$ERROR_INFO}"
      close
      return false
    end

    true
  end

  def get_socket
    return nil unless try_connect unless @socket

    @socket_time = Time.now.to_i
    @socket
  end

  def try_connect
    now = Time.now.to_i

    unless @error_history.empty?
      # wait before re-connecting
      wait = @retry_wait * (2**(@error_history.size - 1))
      return false if now <= @socket_time + wait
    end

    begin
      @socket = @connector.call
      @error_history.clear
      return true

    rescue
      $stderr.puts "connect failed: #{$ERROR_INFO}"
      @error_history << $ERROR_INFO
      @socket_time = now

      if @retry_limit < @error_history.size
        # abort all pending records
        @pending.each do|(time, record)|
          abort_message(time, record)
        end
        @pending.clear
        @error_history.clear
      end

      return false
    end
  end

  def abort_message(time, record)
    $stdout.puts "!#{time}:#{Yajl.dump(record)}"
  end
end

if unix
  connector = proc do
    UNIXSocket.open(socket_path)
  end
else
  connector = proc do
    TCPSocket.new(host, port)
  end
end

w = Writer.new(tag, connector)
w.start

case format
when 'json'
  begin
    while line = $stdin.gets
      record = Yajl.load(line)
      w.write(record)
    end
  rescue
    $stderr.puts $ERROR_INFO
    exit 1
  end

when 'msgpack'
  begin
    u = MessagePack::Unpacker.new($stdin)
    u.each do|record|
      w.write(record)
    end
  rescue EOFError
  rescue
    $stderr.puts $ERROR_INFO
    exit 1
  end

when 'none'
  begin
    while line = $stdin.gets
      record = { message_key => line.chomp }
      w.write(record)
    end
  rescue
    $stderr.puts $ERROR_INFO
    exit 1
  end

else
  $stderr.puts "Unknown format '#{format}'"
  exit 1
end
