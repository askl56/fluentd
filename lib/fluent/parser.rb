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
  require 'fluent/registry'

  class ParserError < StandardError
  end

  class Parser
    include Configurable

    # SET false BEFORE CONFIGURE, to return nil when time not parsed
    # 'configure()' may raise errors for unexpected configurations
    attr_accessor :estimate_current_event

    config_param :keep_time_key, :bool, default: false

    def initialize
      super
      @estimate_current_event = true
    end

    def configure(conf)
      super
    end

    def parse(_text)
      fail NotImplementedError, 'Implement this method in child class'
    end

    # Keep backward compatibility for existing plugins
    def call(*a, &b)
      parse(*a, &b)
    end
  end

  class TextParser
    # Keep backward compatibility for existing plugins
    ParserError = ::Fluent::ParserError

    class TimeParser
      def initialize(time_format)
        @cache1_key = nil
        @cache1_time = nil
        @cache2_key = nil
        @cache2_time = nil
        @parser =
          if time_format
            proc { |value| Time.strptime(value, time_format) }
          else
            Time.method(:parse)
          end
      end

      def parse(value)
        unless value.is_a?(String)
          fail ParserError, "value must be string: #{value}"
        end

        if @cache1_key == value
          return @cache1_time
        elsif @cache2_key == value
          return @cache2_time
        else
          begin
            time = @parser.call(value).to_i
          rescue => e
            raise ParserError, "invalid time format: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
          end
          @cache1_key = @cache2_key
          @cache1_time = @cache2_time
          @cache2_key = value
          @cache2_time = time
          return time
        end
      end
    end

    module TypeConverter
      Converters = {
        'string' => ->(v) { v.to_s },
        'integer' => ->(v) { v.to_i },
        'float' => ->(v) { v.to_f },
        'bool' => lambda do |v|
          case v.downcase
          when 'true', 'yes', '1'
            true
          else
            false
          end
        end,
        'time' => lambda do |v, time_parser|
          time_parser.parse(v)
        end,
        'array' => lambda do |v, delimiter|
          v.to_s.split(delimiter)
        end
      }

      def self.included(klass)
        klass.instance_eval do
          config_param :types, :string, default: nil
          config_param :types_delimiter, :string, default: ','
          config_param :types_label_delimiter, :string, default: ':'
        end
      end

      def configure(conf)
        super

        @type_converters = parse_types_parameter unless @types.nil?
      end

      private

      def convert_type(name, value)
        converter = @type_converters[name]
        converter.nil? ? value : converter.call(value)
      end

      def parse_types_parameter
        converters = {}

        @types.split(@types_delimiter).each do |pattern_name|
          name, type, format = pattern_name.split(@types_label_delimiter, 3)
          fail ConfigError, 'Type is needed' if type.nil?

          case type
          when 'time'
            t_parser = TimeParser.new(format)
            converters[name] = lambda do |v|
              Converters[type].call(v, t_parser)
            end
          when 'array'
            delimiter = format || ','
            converters[name] = lambda do |v|
              Converters[type].call(v, delimiter)
            end
          else
            converters[name] = Converters[type]
          end
        end

        converters
      end
    end

    class RegexpParser < Parser
      include TypeConverter

      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil

      def initialize(regexp, conf = {})
        super()
        @regexp = regexp
        unless conf.empty?
          conf = Config::Element.new('default_regexp_conf', '', conf, []) unless conf.is_a?(Config::Element)
          configure(conf)
        end

        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def configure(conf)
        super
        @time_parser = TimeParser.new(@time_format)
      end

      def patterns
        { 'format' => @regexp, 'time_format' => @time_format }
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        time = nil
        record = {}

        m.names.each do|name|
          if value = m[name]
            case name
            when @time_key
              time = @mutex.synchronize { @time_parser.parse(value) }
              if @keep_time_key
                record[name] = if @type_converters.nil?
                                 value
                               else
                                 convert_type(name, value)
                               end
              end
            else
              record[name] = if @type_converters.nil?
                               value
                             else
                               convert_type(name, value)
                             end
            end
          end
        end

        time ||= Engine.now if @estimate_current_event

        if block_given?
          yield time, record
        else # keep backward compatibility. will be removed at v1
          return time, record
        end
      end
    end

    class JSONParser < Parser
      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil

      def configure(conf)
        super

        unless @time_format.nil?
          @time_parser = TimeParser.new(@time_format)
          @mutex = Mutex.new
        end
      end

      def parse(text)
        record = Yajl.load(text)

        value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
        if value
          if @time_format
            time = @mutex.synchronize { @time_parser.parse(value) }
          else
            begin
              time = value.to_i
            rescue => e
              raise ParserError, "invalid time value: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
            end
          end
        else
          if @estimate_current_event
            time = Engine.now
          else
            time = nil
          end
        end

        if block_given?
          yield time, record
        else
          return time, record
        end
      rescue Yajl::ParseError
        if block_given?
          yield nil, nil
        else
          return nil, nil
        end
      end
    end

    class ValuesParser < Parser
      include TypeConverter

      config_param :keys, default: [] do |val|
        if val.start_with?('[') # This check is enough because keys parameter is simple. No '[' started column name.
          JSON.load(val)
        else
          val.split(',')
        end
      end
      config_param :time_key, :string, default: nil
      config_param :time_format, :string, default: nil
      config_param :null_value_pattern, :string, default: nil
      config_param :null_empty_string, :bool, default: false

      def configure(conf)
        super

        if @time_key && !@keys.include?(@time_key) && @estimate_current_event
          fail ConfigError, "time_key (#{@time_key.inspect}) is not included in keys (#{@keys.inspect})"
        end

        if @time_format && !@time_key
          fail ConfigError, "time_format parameter is ignored because time_key parameter is not set. at #{conf.inspect}"
        end

        @time_parser = TimeParser.new(@time_format)

        if @null_value_pattern
          @null_value_pattern = Regexp.new(@null_value_pattern)
        end

        @mutex = Mutex.new
      end

      def values_map(values)
        record = Hash[keys.zip(values.map { |value| convert_value_to_nil(value) })]

        if @time_key
          value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
          time = if value.nil?
                   Engine.now if @estimate_current_event
                 else
                   @mutex.synchronize { @time_parser.parse(value) }
                 end
        elsif @estimate_current_event
          time = Engine.now
        else
          time = nil
        end

        convert_field_type!(record) if @type_converters

        [time, record]
      end

      private

      def convert_field_type!(record)
        @type_converters.each_key do |key|
          if value = record[key]
            record[key] = convert_type(key, value)
          end
        end
      end

      def convert_value_to_nil(value)
        value = (value == '') ? nil : value if value && @null_empty_string
        if value && @null_value_pattern
          value = ::Fluent::StringUtil.match_regexp(@null_value_pattern, value) ? nil : value
        end
        value
      end
    end

    class TSVParser < ValuesParser
      config_param :delimiter, :string, default: "\t"

      def configure(conf)
        super
        @key_num = @keys.length
      end

      def parse(text)
        if block_given?
          yield values_map(text.split(@delimiter, @key_num))
        else
          return values_map(text.split(@delimiter, @key_num))
        end
      end
    end

    class LabeledTSVParser < ValuesParser
      config_param :delimiter,       :string, default: "\t"
      config_param :label_delimiter, :string, default: ':'
      config_param :time_key, :string, default: 'time'

      def configure(conf)
        conf['keys'] = conf['time_key'] || 'time'
        super(conf)
      end

      def parse(text)
        @keys  = []
        values = []

        text.split(delimiter).each do |pair|
          key, value = pair.split(label_delimiter, 2)
          @keys.push(key)
          values.push(value)
        end

        if block_given?
          yield values_map(values)
        else
          return values_map(values)
        end
      end
    end

    class CSVParser < ValuesParser
      def initialize
        super
        require 'csv'
      end

      def parse(text)
        if block_given?
          yield values_map(CSV.parse_line(text))
        else
          return values_map(CSV.parse_line(text))
        end
      end
    end

    class NoneParser < Parser
      config_param :message_key, :string, default: 'message'

      def parse(text)
        record = {}
        record[@message_key] = text
        time = @estimate_current_event ? Engine.now : nil
        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class ApacheParser < Parser
      REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*?)(?: +\S*)?)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/
      TIME_FORMAT = '%d/%b/%Y:%H:%M:%S %z'

      def initialize
        super
        @time_parser = TimeParser.new(TIME_FORMAT)
        @mutex = Mutex.new
      end

      def patterns
        { 'format' => REGEXP, 'time_format' => TIME_FORMAT }
      end

      def parse(text)
        m = REGEXP.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        host = m['host']
        host = (host == '-') ? nil : host

        user = m['user']
        user = (user == '-') ? nil : user

        time = m['time']
        time = @mutex.synchronize { @time_parser.parse(time) }

        method = m['method']
        path = m['path']

        code = m['code'].to_i
        code = nil if code == 0

        size = m['size']
        size = (size == '-') ? nil : size.to_i

        referer = m['referer']
        referer = (referer == '-') ? nil : referer

        agent = m['agent']
        agent = (agent == '-') ? nil : agent

        record = {
          'host' => host,
          'user' => user,
          'method' => method,
          'path' => path,
          'code' => code,
          'size' => size,
          'referer' => referer,
          'agent' => agent
        }
        record['time'] = m['time'] if @keep_time_key

        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class SyslogParser < Parser
      # From existence TextParser pattern
      REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      # From in_syslog default pattern
      REGEXP_WITH_PRI = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* {1,2}[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/

      config_param :time_format, :string, default: '%b %d %H:%M:%S'
      config_param :with_priority, :bool, default: false

      def initialize
        super
        @mutex = Mutex.new
      end

      def configure(conf)
        super

        @regexp = @with_priority ? REGEXP_WITH_PRI : REGEXP
        @time_parser = TextParser::TimeParser.new(@time_format)
      end

      def patterns
        { 'format' => @regexp, 'time_format' => @time_format }
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        time = nil
        record = {}

        m.names.each do |name|
          if value = m[name]
            case name
            when 'pri'
              record['pri'] = value.to_i
            when 'time'
              time = @mutex.synchronize { @time_parser.parse(value.gsub(/ +/, ' ')) }
              record[name] = value if @keep_time_key
            else
              record[name] = value
            end
          end
        end

        time ||= Engine.now if @estimate_current_event

        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class MultilineParser < Parser
      config_param :format_firstline, :string, default: nil

      FORMAT_MAX_NUM = 20

      def configure(conf)
        super

        formats = parse_formats(conf).compact.map { |f| f[1..-2] }.join
        begin
          @regex = Regexp.new(formats, Regexp::MULTILINE)
          fail 'No named captures' if @regex.named_captures.empty?
          @parser = RegexpParser.new(@regex, conf)
        rescue => e
          raise ConfigError, "Invalid regexp '#{formats}': #{e}"
        end

        if @format_firstline
          check_format_regexp(@format_firstline, 'format_firstline')
          @firstline_regex = Regexp.new(@format_firstline[1..-2])
        end
      end

      def parse(text, &block)
        if block
          @parser.call(text, &block)
        else
          @parser.call(text)
        end
      end

      def has_firstline?
        !!@format_firstline
      end

      def firstline?(text)
        @firstline_regex.match(text)
      end

      private

      def parse_formats(conf)
        check_format_range(conf)

        prev_format = nil
        (1..FORMAT_MAX_NUM).map do |i|
          format = conf["format#{i}"]
          if (i > 1) && prev_format.nil? && !format.nil?
            fail ConfigError, "Jump of format index found. format#{i - 1} is missing."
          end
          prev_format = format
          next if format.nil?

          check_format_regexp(format, "format#{i}")
          format
        end
      end

      def check_format_range(conf)
        invalid_formats = conf.keys.select do |k|
          m = k.match(/^format(\d+)$/)
          m ? !((1..FORMAT_MAX_NUM).include?(m[1].to_i)) : false
        end
        unless invalid_formats.empty?
          fail ConfigError, "Invalid formatN found. N should be 1 - #{FORMAT_MAX_NUM}: " + invalid_formats.join(',')
        end
      end

      def check_format_regexp(format, key)
        if format[0] == '/' && format[-1] == '/'
          begin
            Regexp.new(format[1..-2], Regexp::MULTILINE)
          rescue => e
            raise ConfigError, "Invalid regexp in #{key}: #{e}"
          end
        else
          fail ConfigError, "format should be Regexp, need //, in #{key}: '#{format}'"
        end
      end
    end

    TEMPLATE_REGISTRY = Registry.new(:config_type, 'fluent/plugin/parser_')
    {
      'apache' => proc { RegexpParser.new(/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, 'time_format' => '%d/%b/%Y:%H:%M:%S %z') },
      'apache_error' => proc { RegexpParser.new(/^\[[^ ]* (?<time>[^\]]*)\] \[(?<level>[^\]]*)\](?: \[pid (?<pid>[^\]]*)\])?( \[client (?<client>[^\]]*)\])? (?<message>.*)$/) },
      'apache2' => proc { ApacheParser.new },
      'syslog' => proc { SyslogParser.new },
      'json' => proc { JSONParser.new },
      'tsv' => proc { TSVParser.new },
      'ltsv' => proc { LabeledTSVParser.new },
      'csv' => proc { CSVParser.new },
      'nginx' => proc { RegexpParser.new(/^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*?)(?: +\S*)?)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, 'time_format' => '%d/%b/%Y:%H:%M:%S %z') },
      'none' => proc { NoneParser.new },
      'multiline' => proc { MultilineParser.new }
    }.each do |name, factory|
      TEMPLATE_REGISTRY.register(name, factory)
    end

    def self.register_template(name, regexp_or_proc, time_format = nil)
      if regexp_or_proc.is_a?(Class)
        factory = proc { regexp_or_proc.new }
      elsif regexp_or_proc.is_a?(Regexp)
        regexp = regexp_or_proc
        factory = proc { RegexpParser.new(regexp, 'time_format' => time_format) }
      else
        factory = regexp_or_proc
      end

      TEMPLATE_REGISTRY.register(name, factory)
    end

    def self.lookup(format)
      fail ConfigError, "'format' parameter is required" if format.nil?

      if format[0] == '/' && format[format.length - 1] == '/'
        # regexp
        begin
          regexp = Regexp.new(format[1..-2])
          fail 'No named captures' if regexp.named_captures.empty?
        rescue
          raise ConfigError, "Invalid regexp '#{format[1..-2]}': #{$ERROR_INFO}"
        end

        RegexpParser.new(regexp)
      else
        # built-in template
        begin
          factory = TEMPLATE_REGISTRY.lookup(format)
        rescue ConfigError => e # keep same error message
          raise ConfigError, "Unknown format template '#{format}'"
        end

        factory.call
      end
    end

    def initialize
      @parser = nil
      @estimate_current_event = nil
    end

    attr_reader :parser

    # SET false BEFORE CONFIGURE, to return nil when time not parsed
    # 'configure()' may raise errors for unexpected configurations
    attr_accessor :estimate_current_event

    def configure(conf, _required = true)
      format = conf['format']

      @parser = TextParser.lookup(format)
      if ! @estimate_current_event.nil? && @parser.respond_to?(:'estimate_current_event=')
        @parser.estimate_current_event = @estimate_current_event
      end

      @parser.configure(conf) if @parser.respond_to?(:configure)

      true
    end

    def parse(text, &block)
      if block
        @parser.parse(text, &block)
      else # keep backward compatibility. Will be removed at v1
        return @parser.parse(text)
      end
    end
  end
end
