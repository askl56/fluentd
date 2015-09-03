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
  class MonitorAgentInput < Input
    Plugin.register_input('monitor_agent', self)

    require 'webrick'

    def initialize
      require 'cgi'
      super
    end

    config_param :bind, :string, default: '0.0.0.0'
    config_param :port, :integer, default: 24_220

    class MonitorServlet < WEBrick::HTTPServlet::AbstractServlet
      def initialize(_server, agent)
        @agent = agent
      end

      def do_GET(req, res)
        begin
          code, header, body = process(req, res)
        rescue
          code, header, body = render_json_error(500,               'message ' => 'Internal Server Error',
                                                                    'error' => "#{$ERROR_INFO}",
                                                                    'backgrace' => $ERROR_INFO.backtrace)
        end

        # set response code, header and body
        res.status = code
        header.each_pair do|k, v|
          res[k] = v
        end
        res.body = body
      end

      def build_object(req, _res)
        return render_json_error(404, 'Not found') unless req.path_info == ''

        # parse ?=query string
        if req.query_string
          begin
            qs = CGI.parse(req.query_string)
          rescue
            return render_json_error(400, 'Invalid query string')
          end
        else
          qs = Hash.new { |_h, _k| [] }
        end

        # if ?debug=1 is set, set :with_debug_info for get_monitor_info
        # and :pretty_json for render_json_error
        opts = {}
        if s = qs['debug'] and s[0]
          opts[:with_debug_info] = true
          opts[:pretty_json] = true
        end

        if tag = get_search_parameter(qs, 'tag'.freeze)
          # ?tag= to search an output plugin by match pattern
          if obj = @agent.plugin_info_by_tag(tag, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_id = get_search_parameter(qs, '@id'.freeze)
          # ?@id= to search a plugin by 'id <plugin_id>' config param
          if obj = @agent.plugin_info_by_id(plugin_id, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_id = get_search_parameter(qs, 'id'.freeze)
          # Without @ version of ?@id= for backward compatibility
          if obj = @agent.plugin_info_by_id(plugin_id, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_type = get_search_parameter(qs, '@type'.freeze)
          # ?@type= to search plugins by 'type <type>' config param
          list = @agent.plugins_info_by_type(plugin_type, opts)

        elsif plugin_type = get_search_parameter(qs, 'type'.freeze)
          # Without @ version of ?@type= for backward compatibility
          list = @agent.plugins_info_by_type(plugin_type, opts)

        else
          # otherwise show all plugins
          list = @agent.plugins_info_all(opts)
        end

        [list, opts]
      end

      def get_search_parameter(qs, param_name)
        return nil unless qs.key?(param_name)
        qs[param_name].first
      end

      def render_json(obj, opts = {})
        render_json_error(200, obj, opts)
      end

      def render_json_error(code, obj, opts = {})
        if opts[:pretty_json]
          js = JSON.pretty_generate(obj)
        else
          js = obj.to_json
        end
        [code, { 'Content-Type' => 'application/json' }, js]
      end
    end

    class LTSVMonitorServlet < MonitorServlet
      def process(req, res)
        list, opts = build_object(req, res)
        return unless list

        normalized = JSON.parse(list.to_json)

        text = ''

        normalized.map do|hash|
          row = []
          hash.each_pair do|k, v|
            row << "#{k}:#{v}" unless v.is_a?(Hash) || v.is_a?(Array)
          end
          text << row.join("\t") << "\n"
        end

        [200, { 'Content-Type' => 'text/plain' }, text]
      end
    end

    class JSONMonitorServlet < MonitorServlet
      def process(req, res)
        list, opts = build_object(req, res)
        return unless list

        render_json({
                      'plugins' => list
                    }, opts)
      end
    end

    class ConfigMonitorServlet < MonitorServlet
      def build_object(_req, _res)
        {
          'pid' => Process.pid,
          'ppid' => Process.ppid
        }.merge(@agent.fluentd_opts)
      end
    end

    class LTSVConfigMonitorServlet < ConfigMonitorServlet
      def process(req, res)
        result = build_object(req, res)

        row = []
        JSON.parse(result.to_json).each_pair do |k, v|
          row << "#{k}:#{v}"
        end
        text = row.join("\t")

        [200, { 'Content-Type' => 'text/plain' }, text]
      end
    end

    class JSONConfigMonitorServlet < ConfigMonitorServlet
      def process(req, res)
        result = build_object(req, res)
        render_json(result)
      end
    end

    def start
      log.debug "listening monitoring http server on http://#{@bind}:#{@port}/api/plugins"
      @srv = WEBrick::HTTPServer.new(BindAddress: @bind,
                                     Port: @port,
                                     Logger: WEBrick::Log.new(STDERR, WEBrick::Log::FATAL),
                                     AccessLog: [])
      @srv.mount('/api/plugins', LTSVMonitorServlet, self)
      @srv.mount('/api/plugins.json', JSONMonitorServlet, self)
      @srv.mount('/api/config', LTSVConfigMonitorServlet, self)
      @srv.mount('/api/config.json', JSONConfigMonitorServlet, self)
      @thread = Thread.new do
        @srv.start
      end
    end

    def shutdown
      if @srv
        @srv.shutdown
        @srv = nil
      end
      if @thread
        @thread.join
        @thread = nil
      end
    end

    MONITOR_INFO = {
      'output_plugin' => 'is_a?(::Fluent::Output)', # deprecated. Use plugin_category instead
      'buffer_queue_length' => '@buffer.queue_size',
      'buffer_total_queued_size' => '@buffer.total_queued_chunk_size',
      'retry_count' => '@num_errors'
    }

    def all_plugins
      array = []

      # get all input plugins
      array.concat Engine.root_agent.inputs

      # get all output plugins
      Engine.root_agent.outputs.each do |o|
        MonitorAgentInput.collect_children(o, array)
      end
      # get all filter plugins
      Engine.root_agent.filters.each do |f|
        MonitorAgentInput.collect_children(f, array)
      end
      Engine.root_agent.labels.each do |_name, l|
        # TODO: Add label name to outputs / filters for identifing plugins
        l.outputs.each { |o| MonitorAgentInput.collect_children(o, array) }
        l.filters.each { |f| MonitorAgentInput.collect_children(f, array) }
      end

      array
    end

    # get nexted plugins (such as <store> of the copy plugin)
    # from the plugin `pe` recursively
    def self.collect_children(pe, array = [])
      array << pe
      if pe.is_a?(MultiOutput) && pe.respond_to?(:outputs)
        pe.outputs.each do|nop|
          collect_children(nop, array)
        end
      end
      array
    end

    # try to match the tag and get the info from the matched output plugin
    # TODO: Support output in label
    def plugin_info_by_tag(tag, opts = {})
      matches = Engine.root_agent.event_router.instance_variable_get(:@match_rules)
      matches.each do |rule|
        if rule.match?(tag)
          if rule.collector.is_a?(Output)
            return get_monitor_info(rule.collector, opts)
          end
        end
      end
      nil
    end

    # search a plugin by plugin_id
    def plugin_info_by_id(plugin_id, opts = {})
      found = all_plugins.find do|pe|
        pe.respond_to?(:plugin_id) && pe.plugin_id.to_s == plugin_id
      end
      get_monitor_info(found, opts) if found
    end

    # This method returns an array because
    # multiple plugins could have the same type
    def plugins_info_by_type(type, opts = {})
      array = all_plugins.select do|pe|
        (pe.config['@type'] == type || pe.config['type'] == type) rescue nil
      end
      array.map do|pe|
        get_monitor_info(pe, opts)
      end
    end

    def plugins_info_all(opts = {})
      all_plugins.map do|pe|
        get_monitor_info(pe, opts)
      end
    end

    # get monitor info from the plugin `pe` and return a hash object
    def get_monitor_info(pe, opts = {})
      obj = {}

      # Common plugin information
      obj['plugin_id'] = pe.plugin_id
      obj['plugin_category'] = plugin_category(pe)
      obj['type'] = pe.config['@type'] || pe.config['type']
      obj['config'] = pe.config

      # run MONITOR_INFO in plugins' instance context and store the info to obj
      MONITOR_INFO.each_pair do|key, code|
        begin
          obj[key] = pe.instance_eval(code)
        rescue
        end
      end

      # include all instance variables if :with_debug_info is set
      if opts[:with_debug_info]
        iv = {}
        pe.instance_eval do
          instance_variables.each do|sym|
            key = sym.to_s[1..-1] # removes first '@'
            iv[key] = instance_variable_get(sym)
          end
        end
        obj['instance_variables'] = iv
      end

      obj
    end

    def plugin_category(pe)
      case pe
      when Fluent::Input
        'input'.freeze
      when Fluent::Output
        'output'.freeze
      when Fluent::Filter
        'filter'.freeze
      else
        'unknown'.freeze
      end
    end

    def fluentd_opts
      @fluentd_opts ||= get_fluentd_opts
    end

    def get_fluentd_opts
      opts = {}
      ObjectSpace.each_object(Fluent::Supervisor) do |obj|
        opts.merge!(obj.options)
        break
      end
      opts
    end
  end
end
