# encoding: utf-8
require "logstash/outputs/base"
require "logstash/plugin_mixins/http_client"
require "stud/buffer"
require "zlib"
require "base64"
require "json"

# An sensors_analytics output that does nothing.
class LogStash::Outputs::SensorsAnalytics < LogStash::Outputs::Base
  include Stud::Buffer
  include LogStash::PluginMixins::HttpClient

  attr_accessor :buffer_state

  config_name "sensors_analytics"
  concurrency :single

  # URL to use
  config :url, :validate => :string, :required => :true

  # 数据的项目
  config :project, :validate => :string

  # 触发 flush 间隔
  config :flush_interval_sec, :validate => :number, :default => 2

  # 批次最大 record 数量
  config :flush_batch_size, :validate => :number, :default => 100

  PLUGIN_VERSION = "0.1.1"

  public
  def register
    @logger.info("Registering sensors_analytics Output",
                 :url => @url,
                 :flush_interval => @flush_interval,
                 :flush_batch_size => @flush_batch_size
    )

    http_client_config = client_config
    http_client_config[:user_agent] = "SensorsAnalytics Logstash Output Plugin " + PLUGIN_VERSION
    @client = Manticore::Client.new(http_client_config)

    buffer_config = {
        :max_items => @flush_batch_size.to_i,
        :max_interval => @flush_interval.to_i,
        :logger => @logger
    }

    buffer_initialize(buffer_config)
  end

  public
  def multi_receive(events)
    return if events.empty?

    events.each do |e|
      begin
        record = JSON.parse(e.get("message"))

        host = e.get("host")
        host = host["name"] if host != nil and host.is_a?(Hash)

        path = e.get("path")
        path = e.get("[log][file][path]") if path == nil

        record["lib"] = {
            "$lib" => "Logstash",
            "$lib_version" => PLUGIN_VERSION,
            "$lib_method" => "tools",
            "$lib_detail" => "#{host}###{path}"
        }

        record["project"] = @project if @project != nil

        buffer_receive(record)
      rescue
        @logger.error("Could not process record", :record => e.to_s)
      end
    end
  end

  public
  def close
    buffer_flush(:final => true)
    client.close
  end

  public
  def flush(events, final = false)
    wio = StringIO.new("w")
    gzip_io = Zlib::GzipWriter.new(wio)
    gzip_io.write(events.to_json)
    gzip_io.close
    data = Base64.strict_encode64(wio.string)
    form_data = {"data_list" => data, "gzip" => 1}

    response = client.post(@url, :params => form_data).call
    if response.code != 200
      @logger.warn("Send failed, code: #{response.code}, body: #{response.body}")
      raise
    end
  end

end # class LogStash::Outputs::SensorsAnalytics
