# encoding: utf-8
require "logstash/outputs/base"
require "logstash/plugin_mixins/http_client"
require "zlib"
require "base64"
require "json"
require 'lru_redux'

class Block
  def initialize(index, max_capacity, flush_interval)
    @index = index
    @max_capacity = max_capacity
    @flush_interval = flush_interval
    @empty_list = []
    @pending_events = []
    @outgoing_events = []
    @lock = Mutex.new
    @last_flush = Time.now
  end

  def index
    @index
  end

  def full?
    (@pending_events.length + @outgoing_events.length) >= @max_capacity
  end

  def push(event)
    @lock.synchronize do
      @pending_events << event
    end
  end

  def need_to_flush(force)
    return false if @pending_events.empty?
    return true if force
    full? || (Time.now - @last_flush) >= @flush_interval
  end

  def poll_all
    @lock.synchronize do
      @outgoing_events = @pending_events
      @pending_events = []
    end
    @outgoing_events
  end

  def before_flush
    @lock.synchronize do
      @last_flush = Time.now
      @outgoing_events = @empty_list
    end
  end
end # class Block

module Buffer
  public

  def buffer_initialize(options = {})
    unless self.class.method_defined?(:flush)
      raise ArgumentError, "Any class including Buffer must define a flush() method."
    end

    @flush_interval_sec = options[:flush_interval_sec] || 2
    @flush_batch_size = options[:flush_batch_size] || 100
    @logger = options[:logger] || nil
    @flush_lock = Mutex.new
    @total_send_count = 0
    @url_send_count = {}
    @buffer_blocks = Array.new

    url_list = options[:url_list]
    if url_list.nil? || url_list.empty?
      raise ArgumentError, "url_list can not be cannot be empty."
    end
    url_list.each_index do |i|
      @url_send_count[url_list[i]] = 0
      @buffer_blocks << Block.new(i, @flush_batch_size, @flush_interval_sec)
    end

    # 定时 flush
    @flush_thread = Thread.new do
      loop do
        sleep(@flush_interval_sec)
        buffer_flush
      end
    end
  end

  def block_index(tag)
    return 0 if @buffer_blocks.length == 1
    tag.hash % @buffer_blocks.length
  end

  public

  def buffer_receive(event, tag)
    block_index = block_index(tag)
    block = @buffer_blocks[block_index]
    sleep 0.1 while block.full?
    block.push(event)
    buffer_flush(:single_block => block)
  end

  def flush_blocks(block)
    @flush_lock.lock
    begin
      events = block.poll_all
      begin
        unless events.empty?
          receive_url = flush(events, block.index)
          @url_send_count[receive_url] += events.length
          @total_send_count += events.length
        end
      rescue => e
        @logger.warn("Failed to flush outgoing items",
                     :exception => e.class.name,
                     :backtrace => e.backtrace
        ) if @logger
        sleep 1
        retry
      end
      block.before_flush
    ensure
      @flush_lock.unlock
    end
  end

  public

  def buffer_flush(option = {})
    single_block = option[:single_block]
    force = option[:force] || false
    if single_block.nil?
      @buffer_blocks.each do |block|
        if block.need_to_flush(force)
          flush_blocks(block)
        end
      end
    else
      if single_block.need_to_flush(force)
        flush_blocks(single_block)
      end
    end
  end

end # module Buffer

class LogStash::Outputs::SensorsAnalytics < LogStash::Outputs::Base
  include Buffer
  include LogStash::PluginMixins::HttpClient

  config_name "sensors_analytics"
  concurrency :single

  # 数据接收地址, 可配置多个
  config :url, :validate => :array, :required => :true

  # 数据的项目
  config :project, :validate => :string

  # 触发 flush 间隔
  config :flush_interval_sec, :validate => :number, :default => 2

  # 批次最大 record 数量
  config :flush_batch_size, :validate => :number, :default => 100

  # 数据中用做 hash 的值
  config :hash_filed, :validate => :array

  # 开启 filebeat 状态记录
  config :enable_filebeat_status_report, :validate => :boolean, :default => true

  PLUGIN_VERSION = "0.1.2"

  public

  def register
    @logger.info("Registering sensors_analytics Output",
                 :version => PLUGIN_VERSION,
                 :url => @url,
                 :flush_interval_sec => @flush_interval_sec,
                 :flush_batch_size => @flush_batch_size,
                 :hash_filed => @hash_filed,
                 :enable_filebeat_status_report => @enable_filebeat_status_report
    )

    http_client_config = client_config
    http_client_config[:user_agent] = "SensorsAnalytics Logstash Output Plugin " + PLUGIN_VERSION
    @client = Manticore::Client.new(http_client_config)

    @last_report_count = 0
    @last_report_time = Time.now
    buffer_config = {
        :flush_batch_size => @flush_batch_size,
        :flush_interval_sec => @flush_interval_sec,
        :url_list => @url,
        :logger => @logger
    }

    @recent_filebeat_status = LruRedux::Cache.new(100) if @enable_filebeat_status_report
    @report_thread = Thread.new do
      loop do
        sleep 10
        report
      end
    end

    buffer_initialize(buffer_config)
  end

  public

  def filebeat_input?(events)
    tag = events.get("[agent][type]")
    return true if !tag.nil? && tag == "filebeat"
    tag = events.get("[@metadata][beat]")
    return true if !tag.nil? && tag == "filebeat"
    false
  end

  def concat_tag_from_hash_filed(events)
    if !@hash_filed.nil? && !@hash_filed.empty?
      tag = ""
      @hash_filed.each do |filed|
        tag << events.get(filed).to_s
      end
      return tag
    end
    nil
  end

  public

  def multi_receive(events)
    return if events.empty?

    events.each do |e|
      begin
        record = JSON.parse(e.get("message"))
        tag = concat_tag_from_hash_filed(e)
        if filebeat_input?(e)
          host = e.get("[host][name]")
          file = e.get("[log][file][path]")
          offset = e.get("[log][offset]")
          lib_detail = "#{host}###{file}"
          tag = host.to_s + file.to_s if tag.nil?
          collect_filebeat_status(lib_detail, offset) if @enable_filebeat_status_report
        else
          # 由于 input 种类很多, 一般情况下 @metadata 中总会有有用的信息
          metadata = e.get("@metadata")
          lib_detail = ""
          if metadata.is_a?(Hash)
            metadata.to_hash.each do |k, v|
              lib_detail << "#{k}=#{v}##"
            end
            lib_detail = lib_detail.chop.chop
          end
        end

        record["lib"] = {
            "$lib" => "Logstash",
            "$lib_version" => PLUGIN_VERSION,
            "$lib_method" => "tools",
            "$lib_detail" => lib_detail
        }

        record["project"] = @project if @project != nil

        buffer_receive(record, tag)
      rescue
        @logger.error("Could not process record", :record => e.to_s)
      end
    end
  end

  public

  def close
    @report_thread.kill unless @report_thread.nil?
    @flush_thread.kill unless @flush_thread.nil?
    buffer_flush(:force => true)
    client.close
    report
  end

  public

  def do_send(form_data, url)
    begin
      response = client.post(url, :params => form_data).call
      if response.code != 200
        @logger.warn("Send failed, code: #{response.code}, body: #{response.body}")
        return false
      end
    rescue => e
      @logger.warn("Send failed", :exception => e.class.name, :backtrace => e.backtrace)
      return false
    end
    true
  end

  def flush(events, index)
    wio = StringIO.new("w")
    gzip_io = Zlib::GzipWriter.new(wio)
    gzip_io.write(events.to_json)
    gzip_io.close
    data = Base64.strict_encode64(wio.string)
    form_data = {"data_list" => data, "gzip" => 1}

    current_index = index
    url = @url[current_index]
    # 当发送失败时，轮流发送其他 url, 直到成功
    until do_send(form_data, url) do
      last_url = url
      if current_index < @url.length - 1
        current_index += 1
      else
        current_index = 0
      end
      sleep 1 if current_index == index
      url = @url[current_index]
      @logger.warn("send failed, retry send data to another url", :last_url => last_url, :retry_url => url)
    end
    url
  end

  # 在 concurrency : single 的情况下 multi_receive 是线程安全的, 因此计数应该是准确的
  def collect_filebeat_status(lib_detail, offset)
    status = @recent_filebeat_status[lib_detail]
    if status.nil?
      status = {:receive_time => Time.now, :count => 1, :offset => offset}
      @recent_filebeat_status[lib_detail] = status
    else
      status[:count] += 1
      status[:offset] = offset
      status[:receive_time] = Time.now
    end
  end

  def format_filebeat_report
    result = "\n"
    @recent_filebeat_status.each do |k, v|
      result << k << "=>" << v.to_s << "\n"
    end
    result
  end

  def report
    speed = (@total_send_count - @last_report_count) / (Time.now - @last_report_time)
    @last_report_count = @total_send_count
    @last_report_time = Time.now
    @logger.info("Report",
                 :speed => speed.round(2),
                 :total_send_count => @total_send_count,
                 :url_send_count => @url_send_count)
    @logger.info("Filebeat status Report: #{format_filebeat_report}") if @enable_filebeat_status_report
  end

end # class LogStash::Outputs::SensorsAnalytics

