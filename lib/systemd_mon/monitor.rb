require 'thread'
require 'systemd_mon/logger'
require 'systemd_mon/callback_manager'
require 'systemd_mon/notification_centre'
require 'systemd_mon/notification'
require 'systemd_mon/error'

module SystemdMon
  class Monitor
    def initialize(dbus_manager)
      self.hostname     = `hostname`.strip
      self.dbus_manager = dbus_manager
      self.units        = []
      self.change_callback     = lambda(&method(:unit_change_callback))
      self.notification_centre = NotificationCentre.new
      Thread.abort_on_exception = true
    end

    def add_notifier(notifier)
      notification_centre << notifier
      self
    end

    def register_unit(unit_name)
      self.units << dbus_manager.fetch_unit(unit_name)
      self
    end

    def register_units(*unit_names)
      self.units.concat unit_names.flatten.map { |unit_name|
        dbus_manager.fetch_unit(unit_name)
      }
      self
    end

    def on_change(&callback)
      self.change_callback = callback
      self
    end

    def on_each_state_change(&callback)
      self.each_state_change_callback = callback
      self
    end

    def start
      startup_check!
      at_exit { notification_centre.notify_stop! hostname }
      notification_centre.notify_start! hostname

      Logger.debug { "Using notifiers: #{notification_centre.classes.join(", ")}"}

      state_q = Queue.new
      unit_q = Queue.new

      dbus_manager.discover_units do |unit|
        dbus_unit = dbus_manager.fetch_unit unit
        units << dbus_unit
        dbus_unit.register_listener! state_q
      end

      dbus_manager.watch_for_new_units

      [start_callback_thread(state_q),
       start_manager_adder_thread(unit_q),
       start_monitor_adder_thread(unit_q),
       start_dbus_thread].each(&:join)
    end

protected
    attr_accessor :units, :dbus_manager, :change_callback, :each_state_change_callback, :hostname, :notification_centre

    def startup_check!
      unless notification_centre.any?
        raise MonitorError, "At least one notifier should be registered before monitoring can start"
      end
      self
    end

    def start_dbus_thread
      Thread.new do
        dbus_manager.runner.run
      end
    end

    def start_callback_thread(state_q)
      Thread.new do
        manager = CallbackManager.new(state_q)
        manager.start change_callback, each_state_change_callback
      end
    end

    def start_manager_adder_thread(unit_q)
      Thread.new do
        dbus_manager.new_unit_adder unit_q
      end
    end

    def start_monitor_adder_thread(unit_q)
      Thread.new do
        loop do
          action, unit = unit_q.deq

          case action
          when 'add'
            Logger.debug "#{unit} is mon'd, monitoring"
            register_unit unit
          else # remove
            if runit = units.find { |u| u.name == unit }
              Logger.debug "halting monitor for #{unit}"
              units.delete(runit)
            end
          end
        end
      end
    end

    def unit_change_callback(unit)
      Logger.puts "#{unit.name} #{unit.state_change.status_text}: #{unit.state.active} (#{unit.state.sub})"
      Logger.debug unit.state_change.to_s
      Logger.puts
      notification_centre.notify! Notification.new(hostname, unit)
    end
  end
end
