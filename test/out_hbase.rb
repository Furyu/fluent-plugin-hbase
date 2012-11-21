require 'fluent/test'
require 'fluent/plugin/out_hbase'

class HBaseOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    tag_column_name event:tag
    time_column_name event:time
    fields_to_columns_mapping foo=>event:foo,iam.nested=>event:nested
    hbase_host localhost
    hbase_port 9090
    hbase_table events
    buffer_type memory
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::HBaseOutput) do
      # We don't want to connect the HBase instance while testing
      def start
        super
      end

      # prevents writes to the HBase instance while testing
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'event:tag', d.instance.tag_column_name
    assert_equal 'event:time', d.instance.time_column_name
    assert_equal 'foo=>event:foo,iam.nested=>event:nested', d.instance.fields_to_columns_mapping
    assert_equal 'localhost', d.instance.hbase_host
    assert_equal 9090, d.instance.hbase_port
    assert_equal 'events', d.instance.hbase_table
  end

  def test_format
    d = create_driver

    time_in_int = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.emit(
        {
            "foo" => "foo1",
            "iam" => {
                "nested" => "nested1"
            }
        },
        time_in_int
    )

    d.emit(
        {
            "foo" => "foo2",
            "iam" => {
                "nested" => "nested2"
            }
        },
        time_in_int
    )

    expected1 = {
        "event:tag" => "test",
        "event:time" => time_in_int,
        "event:foo" => "foo1",
        "event:nested" => "nested1"
    }.to_msgpack

    expected2 = {
        "event:tag" => "test",
        "event:time" => time_in_int,
        "event:foo" => "foo2",
        "event:nested" => "nested2"
    }.to_msgpack

    d.expect_format expected1
    d.expect_format expected2

    d.run
  end

  def test_write
    d = create_driver

    time_in_int = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.emit(
        {
            "foo" => "foo1",
            "iam" => {
                "nested" => "nested1"
            }
        },
        time_in_int
    )

    d.emit(
        {
            "foo" => "foo2",
            "iam" => {
                "nested" => "nested2"
            }
        },
        time_in_int
    )

    expected1 = {
        "event:tag" => "test",
        "event:time" => time_in_int,
        "event:foo" => "foo1",
        "event:nested" => "nested1"
    }.to_msgpack

    expected2 = {
        "event:tag" => "test",
        "event:time" => time_in_int,
        "event:foo" => "foo2",
        "event:nested" => "nested2"
    }.to_msgpack

    # HBaseOutputTest#write returns chunk.read
    data = d.run

    assert_equal expected1 + expected2, data
  end

end

