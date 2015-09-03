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
  require 'fluent/config/error'

  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, unused = nil)
        @name = name
        @arg = arg
        @elements = elements
        super()
        attrs.each do |k, v|
          self[k] = v
        end
        @unused = unused || attrs.keys
        @v1_config = false
        @corresponding_proxies = [] # some plugins use flat parameters, e.g. in_http doesn't provide <format> section for parser.
        @unused_in = false # if this element is not used in plugins, correspoing plugin name and parent element name is set, e.g. [source, plugin class].
      end

      attr_accessor :name, :arg, :elements, :unused, :v1_config, :corresponding_proxies, :unused_in

      def add_element(name, arg = '')
        e = Element.new(name, arg, {}, [])
        e.v1_config = @v1_config
        @elements << e
        e
      end

      def inspect
        attrs = super
        "name:#{@name}, arg:#{@arg}, " + attrs + ', ' + @elements.inspect
      end

      # This method assumes _o_ is an Element object. Should return false for nil or other object
      def ==(o)
        name == o.name && arg == o.arg &&
          keys.size == o.keys.size &&
          keys.reduce(true) { |r, k| r && self[k] == o[k] } &&
          elements.size == o.elements.size &&
          [elements, o.elements].transpose.reduce(true) { |r, e| r && e[0] == e[1] }
      end

      def +(o)
        e = Element.new(@name.dup, @arg.dup, o.merge(self), @elements + o.elements, (@unused + o.unused).uniq)
        e.v1_config = @v1_config
        e
      end

      def each_element(*names, &block)
        if names.empty?
          @elements.each(&block)
        else
          @elements.each do |e|
            block.yield(e) if names.include?(e.name)
          end
        end
      end

      def has_key?(key)
        @unused.delete(key)
        super
      end

      def [](key)
        @unused.delete(key)
        super
      end

      def check_not_fetched(&block)
        each_key do |key|
          block.call(key, self) if @unused.include?(key)
        end
        @elements.each do |e|
          e.check_not_fetched(&block)
        end
      end

      def to_s(nest = 0)
        indent = '  ' * nest
        nindent = '  ' * (nest + 1)
        out = ''
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end
        each_pair do |k, v|
          if secret_param?(k)
            out << "#{nindent}#{k} xxxxxx\n"
          else
            out << "#{nindent}#{k} #{v}\n"
          end
        end
        @elements.each do |e|
          out << e.to_s(nest + 1)
        end
        out << "#{indent}</#{@name}>\n"
        out
      end

      def to_masked_element
        new_elems = @elements.map(&:to_masked_element)
        new_elem = Element.new(@name, @arg, {}, new_elems, @unused)
        each_pair do |k, v|
          new_elem[k] = secret_param?(k) ? 'xxxxxx' : v
        end
        new_elem
      end

      def secret_param?(key)
        return false if @corresponding_proxies.empty?

        param_key = key.to_sym
        @corresponding_proxies.each do |proxy|
          block, opts = proxy.params[param_key]
          return opts[:secret] if opts && opts.key?(:secret)
        end

        false
      end

      def self.unescape_parameter(v)
        result = ''
        v.each_char { |c| result << LiteralParser.unescape_char(c) }
        result
      end
    end
  end
end
