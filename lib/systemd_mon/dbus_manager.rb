require 'dbus'
require 'thread'
require 'systemd_mon/error'
require 'systemd_mon/dbus_unit'

module SystemdMon
  class DBusManager
    def initialize
      self.dbus            = DBus::SystemBus.instance
      self.systemd_events  = Queue.new
      self.tentative_units = {}
      self.systemd_service = dbus.service("org.freedesktop.systemd1")
      self.systemd_object  = systemd_service.object("/org/freedesktop/systemd1")
      systemd_object.introspect
      systemd_object.default_iface = 'org.freedesktop.systemd1.Manager'
      if systemd_object.respond_to?("Subscribe")
        systemd_object.Subscribe
      else
        raise SystemdMon::SystemdError, "Systemd is not installed, or is an incompatible version. It must provide the Subscribe dbus method: version 204 is the minimum recommended version."
      end
    end

    def service?(unit_name)
      unit_name.end_with? '.service'
    end

    def discover_units
      systemd_object.ListUnits().first.select do |unit|
        name = unit.first
        yield name if monitored_service? name
      end
    end

    def monitored_service?(unit_name)
      if service? unit_name
        Logger.debug "Checking env flag for #{unit_name}"
        path = systemd_object.GetUnit(unit_name).first
        Logger.debug "Got path for #{unit_name}: #{path}"
        unit_object = systemd_service.object(path)
        Logger.debug "Got object for #{unit_name}"
        unit_object.introspect
        Logger.debug "Introspected #{unit_name}"
        iface = unit_object['org.freedesktop.systemd1.Service']
        iface['Environment'].each do |pair|
          var, val = pair.downcase.split('=')
          if var == 'monitor_dbus' and
            ['yes', '1', 'true'].include? val
              return true
          end
        end
      end

      false
    end

    def watch_for_new_units
      systemd_object.on_signal('UnitNew') do |name, opath|
        systemd_events.enq [name, 'new', Time.now, opath]
      end
      systemd_object.on_signal('UnitRemoved') do |name, opath|
        systemd_events.enq [name, 'removed', Time.now, opath]
      end
    end

    # Every five seconds, this wakes up and dequeues all systemd
    # events. If the new/removed signal has settled, we either add
    # or remove the unit from our list of units to monitor.
    def new_unit_adder(unit_q)

      loop do
        sleep 5

        pruned_units = []

        while not systemd_events.empty?
          uname, change, ts, opath = systemd_events.deq

          if tentative_units.has_key? uname
            tentative_units[uname][:last_seen]   = ts
            tentative_units[uname][:last_change] = change
          else
            tentative_units[uname] = {
              :last_seen   => ts,
              :last_change => change,
              :path        => opath,
            }
          end
        end # of systemd_events dequeue

        tentative_units.each_pair do |name, cache|
          if Time.now - cache[:last_seen] > 5
            case cache[:last_change]
            when 'new'
              if monitored_service? name
                unit_q << ['add', name]
              end
            else # removed
              unit_q << ['remove', name]
            end

            pruned_units << name
          end # if settle delay has elapsed
        end # iteration of tentative_units

        pruned_units.each { |u| tentative_units.delete(u) }
      end
    end

    def fetch_unit(unit_name)
      path = systemd_object.GetUnit(unit_name).first
      DBusUnit.new(unit_name, path, systemd_service.object(path))
    rescue DBus::Error
      raise SystemdMon::UnknownUnitError, "Unknown or unloaded systemd unit '#{unit_name}'"
    end

    def runner
      main = DBus::Main.new
      main << dbus
      main
    end

  protected
    attr_accessor :systemd_service, :systemd_object, :dbus, :systemd_events, :tentative_units
  end
end
