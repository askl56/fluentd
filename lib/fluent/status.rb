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
  class StatusClass
    def initialize
      @entries = {}
      @mutex = Mutex.new
    end

    def register(instance, name, &block)
      @mutex.synchronize do
        (@entries[instance.object_id] ||= {})[name] = block
      end
      nil
    end

    def each(&block)
      @mutex.synchronize do
        @entries.each do|_obj_id, hash|
          record = {}
          hash.each_pair do|name, block|
            record[name] = block.call
          end
          block.call(record)
        end
      end
    end
  end

  # Don't use this class from plugins.
  # The interface may be changed
  Status = StatusClass.new
end
