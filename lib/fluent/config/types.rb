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

module Fluent
  require 'json'

  module Config
    def self.size_value(str)
      case str.to_s
      when /([0-9]+)k/i
        $LAST_MATCH_INFO[1].to_i * 1024
      when /([0-9]+)m/i
        $LAST_MATCH_INFO[1].to_i * (1024**2)
      when /([0-9]+)g/i
        $LAST_MATCH_INFO[1].to_i * (1024**3)
      when /([0-9]+)t/i
        $LAST_MATCH_INFO[1].to_i * (1024**4)
      else
        str.to_i
      end
    end

    def self.time_value(str)
      case str.to_s
      when /([0-9]+)s/
        $LAST_MATCH_INFO[1].to_i
      when /([0-9]+)m/
        $LAST_MATCH_INFO[1].to_i * 60
      when /([0-9]+)h/
        $LAST_MATCH_INFO[1].to_i * 60 * 60
      when /([0-9]+)d/
        $LAST_MATCH_INFO[1].to_i * 24 * 60 * 60
      else
        str.to_f
      end
    end

    def self.bool_value(str)
      return nil if str.nil?
      case str.to_s
      when 'true', 'yes'
        true
      when 'false', 'no'
        false
      when ''
        true
      end
    end
  end

  Configurable.register_type(:string, proc do |val, _opts|
    val
  end)

  Configurable.register_type(:enum, proc do |val, opts|
    s = val.to_sym
    fail "Plugin BUG: config type 'enum' requires :list argument" unless opts[:list].is_a?(Array)
    unless opts[:list].include?(s)
      fail ConfigError, "valid options are #{opts[:list].join(',')} but got #{val}"
    end
    s
  end)

  Configurable.register_type(:integer, proc do |val, _opts|
    val.to_i
  end)

  Configurable.register_type(:float, proc do |val, _opts|
    val.to_f
  end)

  Configurable.register_type(:size, proc do |val, _opts|
    Config.size_value(val)
  end)

  Configurable.register_type(:bool, proc do |val, _opts|
    Config.bool_value(val)
  end)

  Configurable.register_type(:time, proc do |val, _opts|
    Config.time_value(val)
  end)

  Configurable.register_type(:hash, proc do |val, _opts|
    param = val.is_a?(String) ? JSON.load(val) : val
    if param.class != Hash
      fail ConfigError, "hash required but got #{val.inspect}"
    end
    param
  end)

  Configurable.register_type(:array, proc do |val, _opts|
    param = val.is_a?(String) ? JSON.load(val) : val
    if param.class != Array
      fail ConfigError, "array required but got #{val.inspect}"
    end
    param
  end)
end
