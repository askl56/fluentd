require_relative 'helper'

module FluentTest
  class FluentTestInput < ::Fluent::Input
    ::Fluent::Plugin.register_input('test_in', self)

    attr_reader :started

    def start
      @started = true
    end

    def shutdown
      @started = false
    end
  end

  class FluentTestOutput < ::Fluent::Output
    ::Fluent::Plugin.register_output('test_out', self)

    def initialize
      super
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events
    attr_reader :started

    def start
      @started = true
    end

    def shutdown
      @started = false
    end

    def emit(tag, es, _chain)
      es.each do |_time, record|
        @events[tag] << record
      end
    end
  end

  class FluentTestErrorOutput < ::Fluent::BufferedOutput
    ::Fluent::Plugin.register_output('test_out_error', self)

    def format(_tag, _time, _record)
      fail 'emit error!'
    end

    def write(_chunk)
      fail 'chunk error!'
    end
  end

  class FluentTestFilter < ::Fluent::Filter
    ::Fluent::Plugin.register_filter('test_filter', self)

    def initialize(field = '__test__')
      super()
      @num = 0
      @field = field
    end

    attr_reader :num
    attr_reader :started

    def start
      @started = true
    end

    def shutdown
      @started = false
    end

    def filter(_tag, _time, record)
      record[@field] = @num
      @num += 1
      record
    end
  end

  class TestEmitErrorHandler
    def initialize
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events

    def handle_emit_error(tag, _time, record, _error)
      @events[tag] << record
    end

    def handle_emits_error(tag, es, error)
      es.each { |time, record| handle_emit_error(tag, time, record, error) }
    end
  end
end
