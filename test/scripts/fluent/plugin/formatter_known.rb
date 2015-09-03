module Fluent
  TextFormatter.register_template('known_old', proc do |tag, time, record|
    "#{tag}:#{time}:#{record.size}"
  end)
  Plugin.register_formatter('known', proc do |tag, time, record|
    "#{tag}:#{time}:#{record.size}"
  end)
end
