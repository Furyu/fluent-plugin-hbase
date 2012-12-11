module Fluent

  class HBaseOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('hbase', self)

    def initialize
      super
      require 'massive_record'
    end

    # Format dates with ISO 8606 by default
    # http://www.w3.org/TR/NOTE-datetime
    config_param :time_format, :string, :default => '%Y-%m-%dT%H:%M:%S.%L%:z'

    include SetTagKeyMixin
    config_set_default :include_tag_key, false
    config_set_default :tag_key, nil

    include SetTimeKeyMixin
    config_set_default :include_time_key, false
    config_set_default :time_key, nil

    config_param :tag_column_name, :string, :default => nil
    config_param :time_column_name, :string, :default => nil
    config_param :fields_to_columns_mapping, :string
    config_param :hbase_host, :string, :default => 'localhost'
    config_param :hbase_port, :integer, :default => 9090
    config_param :hbase_table, :string

    def configure(conf)
      super

      @fields_to_columns = @fields_to_columns_mapping.split(",").map { |src_to_dst|
        src_to_dst.split("=>")
      }
      @mapping = Hash[*@fields_to_columns.flatten]
    end

    def start
      super

      @conn = MassiveRecord::Wrapper::Connection.new(:host => @hbase_host, :port => @hbase_port)
      @table = MassiveRecord::Wrapper::Table.new(@conn, @hbase_table.intern)

      unless @table.exists?
        columns = ([@tag_column_name, @time_column_name] + @mapping.values).reject(&:nil?)
        column_families = columns.map {|column_family_with_column|
          column_family, column = column_family_with_column.split(":")

          if column.nil? or column_family.nil?
            raise <<MESSAGE
Unexpected format for column name: #{column_family_with_column}
Each destination column in the 'record_to_columns_mapping' option
must be specified in the format of \"column_family:column\".
Are you sure you included ':' in column names?
MESSAGE
          end

          column_family.intern
        }
        column_families.uniq!

        @table.create_column_families(column_families)
        @table.save
      end
    end

    def format(tag, time, record)
      row_values = {}

      row_values[@tag_column_name] = tag unless @tag_column_name.nil?
      row_values[@time_column_name] = time unless @time_column_name.nil?

      @fields_to_columns.each {|field,column|

        next if field.nil? or column.nil?

        components = field.split(".")
        value = record
        for c in components
          value = value[c]

          break if value.nil?
        end

        row_values[column] = value
      }

      row_values.to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each {|row_values|
        event = {}

        row_values.each {|column_family_and_column, value|
          column_family, column = column_family_and_column.split(":")

          (event[column_family.intern] ||= {}).update({column => value})
        }

        row = MassiveRecord::Wrapper::Row.new
        row.id = SecureRandom.uuid
        row.values = event
        row.table = @table
        row.save
      }
    end

  end

end
