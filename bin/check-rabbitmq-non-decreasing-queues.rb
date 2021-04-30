#!/usr/bin/env ruby
# frozen_string_literal: true

#
# Check RabbitMQ Messages
# ===
#
# DESCRIPTION:
# This plugin checks when the messages in a queue are not decrementing
#
# PLATFORMS:
#   Linux, BSD, Solaris
#
# DEPENDENCIES:
#   RabbitMQ rabbitmq_management plugin
#   gem: sensu-plugin
#   gem: carrot-top
#
# LICENSE:
# Copyright 2012 Evan Hazlett <ejhazlett@gmail.com>
# Copyright 2015 Tim Smith <tim@cozy.co> and Cozy Services Ltd.
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.

require 'sensu-plugin/check/cli'
require 'socket'
require 'carrot-top'
require 'inifile'
require 'date'
require 'json'
# main plugin class
class CheckRabbitMQMessages < Sensu::Plugin::Check::CLI
  option :host,
         description: 'RabbitMQ management API host',
         long: '--host HOST',
         default: 'localhost'

  option :port,
         description: 'RabbitMQ management API port',
         long: '--port PORT',
         proc: proc(&:to_i),
         default: 15_672

  option :username,
         description: 'RabbitMQ management API user',
         long: '--username USER',
         default: 'guest'

  option :password,
         description: 'RabbitMQ management API password',
         long: '--password PASSWORD',
         default: 'guest'

  option :ssl,
         description: 'Enable SSL for connection to the API',
         long: '--ssl',
         boolean: true,
         default: false

  option :warn,
         short: '-w NUM_MESSAGES',
         long: '--warn NUM_MESSAGES',
         description: 'WARNING message count threshold',
         default: 250

  option :critical,
         short: '-c NUM_MESSAGES',
         long: '--critical NUM_MESSAGES',
         description: 'CRITICAL message count threshold',
         default: 500

  option :queuelevel,
         short: '-q',
         long: '--queuelevel',
         description: 'Monitors that no individual queue is above the thresholds specified'

  option :excluded,
         short: '-e queue_name',
         long: '--excludedqueues queue_name',
         description: 'Comma separated list of queues to exclude when using queue level monitoring',
         proc: proc { |q| q.split(',') },
         default: []

  option :ini,
         description: 'Configuration ini file',
         short: '-i',
         long: '--ini VALUE'

  option :max_crit_non_decreasing_minutes,
         short: '-maxcritminutes NON_DECREASING_MINUTES',
         long: '--max_crit_nondecreasing_minutes NON_DECREASING_MINUTES',
         description: 'CRITICAL queue non decreasing minutes',
         default: 10

  option :max_warn_non_decreasing_minutes,
         short: '-maxwarnminutes NON_DECREASING_MINUTES',
         long: '--max_warn_nondecreasing_minutes NON_DECREASING_MINUTES',
         description: 'WARNING queue non decreasing minutes',
         default: 3

  option :accepted_max_value,
         short: '-acctp_max_val ACCEPTED_MAX_VALUE',
         long: '--accepted_max_value ACCEPTED_MAX_VALUE',
         description: 'Max accepted non decreasing value',
         default: 50

  def generate_message(status_hash)
    message = []
    status_hash.each_pair do |k, v|
      message << "#{k}: #{v}"
    end
    message.join(', ')
  end

  def acquire_rabbitmq_info
    begin
      if config[:ini]
        ini = IniFile.load(config[:ini])
        section = ini['auth']
        username = section['username']
        password = section['password']
      else
        username = config[:username]
        password = config[:password]
      end

      rabbitmq_info = CarrotTop.new(
        host: config[:host],
        port: config[:port],
        user: username,
        password: password,
        ssl: config[:ssl]
      )
    rescue StandardError
      warning 'Could not connect to rabbitmq'
    end
    rabbitmq_info
  end

  def run
    rabbitmq = acquire_rabbitmq_info
    filename = '/tmp/rabbitmq_queues_registry.log'
    max_crit_minutes_limit = config[:max_crit_non_decreasing_minutes].to_i
    max_warn_minutes_limit = config[:max_warn_non_decreasing_minutes].to_i
    max_accepted_value = config[:accepted_max_value].to_i
    # monitor counts in each queue
    crit_queues = {}
    warn_queues = {}
    queues_hash = {}
    now_str = Time.new.inspect
    # Fill queues_hash with current queue info from rabbitmq plugin
    rabbitmq.queues.each do |queue|
      queues_hash[(queue['name']).to_s] = { 'last_decrease' => now_str, 'last_value' => queue['messages'] }
    end
    # puts "#{queues_hash.to_json}"
    # If file exists compare time and messages count
    if File.exist?(filename)
      file = File.read(filename)
      queues_register = JSON.parse(file) # Get data from file log
      now = DateTime.parse(now_str)
      queues_hash.each do |queue_name, hash_data|
        if queues_register.key?(queue_name)
          # next if hash_data["last_value"] <= max accepted value
          if hash_data['last_value'] <= max_accepted_value
            queues_register[queue_name]['last_decrease'] = hash_data['last_decrease']
            queues_register[queue_name]['last_value'] = hash_data['last_value']
            next
          end
          if hash_data['last_value'] >= queues_register[queue_name]['last_value']
            start_time = DateTime.parse(queues_register[queue_name]['last_decrease'])
            elapsed_minutes = ((now - start_time) * 24 * 60).to_i
            # puts "Debugging elapsed minutes #{elapsed_minutes}"
            if elapsed_minutes >= max_crit_minutes_limit
              crit_queues[queue_name] = hash_data['last_value']
            end
            if elapsed_minutes >= max_warn_minutes_limit
              warn_queues[queue_name] = hash_data['last_value']
            end
          else
            queues_register[queue_name]['last_decrease'] = hash_data['last_decrease']
          end
          # Always update last value
          queues_register[queue_name]['last_value'] = hash_data['last_value']
        else
          queues_register[queue_name] = { 'last_decrease' => hash_data['last_decrease'], 'last_value' => hash_data['last_value'] }
        end
      end
    # Updating queues log
      File.open(filename, 'w') do |f|
        f.write(queues_register.to_json)
      end
      critical "Queues non decreasing #{generate_message(crit_queues)} for more than #{max_crit_minutes_limit} minutes" unless crit_queues.empty?
      warning "Queues non decreasing #{generate_message(warn_queues)} for more than #{max_warn_minutes_limit} minutes" unless warn_queues.empty?
      ok 'All Queues OK'
    else
      File.open(filename,'w') do |f|
        f.write(queues_hash.to_json)
      end
      ok 'Log File created'
    end
  end
end
