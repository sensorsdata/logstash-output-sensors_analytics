# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/sensors_analytics"
require "logstash/codecs/plain"
require "logstash/event"
require "base64"

PORT = rand(65535 - 1024) + 1025

class TestApp < Sinatra::Base
  # disable WEBrick logging
  def self.server_settings
    {:AccessLog => [], :Logger => WEBrick::BasicLog::new(nil, WEBrick::BasicLog::FATAL)}
  end

  def self.multiroute(methods, path, &block)
    methods.each do |method|
      method.to_sym
      self.send method, path, &block
    end
  end

  def self.receive_count=(count)
    @receive_count = count
  end

  def self.receive_count
    @receive_count || 0
  end

  def self.mult_0=(count)
    @mult_0 = count
  end

  def self.mult_0
    @mult_0 || 0
  end

  def self.mult_1=(count)
    @mult_1 = count
  end

  def self.mult_1
    @mult_1 || 0
  end

  def self.mult_2=(count)
    @mult_2 = count
  end

  def self.mult_2
    @mult_2 || 0
  end

  def self.last_request=(request)
    @last_request = request
  end

  def self.last_request
    @last_request
  end

  def self.retry_fail_count=(count)
    @retry_fail_count = count
  end

  def self.retry_fail_count
    @retry_fail_count || 2
  end

  def self.final_success=(final_success)
    @final_success = final_success
  end

  def self.final_success
    @final_success || false
  end

  def self.retry_action_count=(request)
    @retry_action_count = request
  end

  def self.retry_action_count
    @retry_action_count || 0
  end

  multiroute(%w(post), "/good") do
    self.class.last_request = request
    self.class.receive_count += 1
    [200, "YUP"]
  end

  multiroute(%w(post), "/mult_0") do
    self.class.last_request = request
    self.class.receive_count += 1
    self.class.mult_0 += 1
    [200, "YUP"]
  end

  multiroute(%w(post), "/mult_1") do
    self.class.last_request = request
    self.class.receive_count += 1
    self.class.mult_1 += 1
    [200, "YUP"]
  end

  multiroute(%w(post), "/mult_2") do
    self.class.last_request = request
    self.class.receive_count += 1
    self.class.mult_2 += 1
    [200, "YUP"]
  end

  multiroute(%w(post), "/bad") do
    self.class.last_request = request
    [400, "YUP"]
  end

  multiroute(%w(post), "/retry") do
    self.class.last_request = request
    self.class.retry_action_count += 1
    if self.class.retry_fail_count > 0
      self.class.retry_fail_count -= 1
      [502, "Will succeed in #{self.class.retry_fail_count}"]
    else
      self.class.final_success = true
      [200, "Done Retrying"]
    end
  end
end

RSpec.configure do |config|
  # http://stackoverflow.com/questions/6557079/start-and-call-ruby-http-server-in-the-same-script
  def sinatra_run_wait(app, opts)
    queue = Queue.new

    t = java.lang.Thread.new(
        proc do
          begin
            app.run!(opts) do |server|
              queue.push("started")
            end
          rescue => e
            puts "Error in webserver thread #{e}"
            # ignore
          end
        end
    )
    t.daemon = true
    t.start
    queue.pop # blocks until the run! callback runs
  end

  config.before(:suite) do
    sinatra_run_wait(TestApp, :port => PORT, :server => 'webrick')
    puts "Test webserver on port #{PORT}"
  end
end


describe LogStash::Outputs::SensorsAnalytics do

  # 解析 request body
  def parse_data(request_data)
    data = request_data[request_data.index("=") + 1...request_data.index("&gzip")]
    url_decode = URI::decode(data)
    base_64 = Base64.strict_decode64(url_decode)
    io = StringIO.new(base_64)
    gzip_io = Zlib::GzipReader.new(io)
    json_str = gzip_io.read
    JSON.parse(json_str)
  end

  describe "basic test" do

    let(:port) { PORT }
    let(:event) {
      event = LogStash::Event.new
      event.set("path", "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/a")
      event.set("host", "LaputadeMacBook-Pro.local")
      event.set("message", '{"distinct_id":"123456","time":1434556935000,"type":"track","event":"ViewProduct","properties":{"product_id":12345,"product_name":"苹果","product_classify":"水果","product_price":14}}')
      event
    }
    let(:method) { "post" }

    context 'sending no events' do
      let(:url) { "http://localhost:#{port}/good" }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 1} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      before do
        subject.register
      end

      it 'should not block the pipeline' do
        subject.multi_receive([])
      end
    end

    context "send events" do
      let(:url) { "http://localhost:#{port}/good" }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 10, "flush_interval_sec" => 2} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      before do
        subject.register
        TestApp.receive_count = 0
      end

      it 'should send requests 3 times' do
        30.times { subject.multi_receive([event]) }
        expect(TestApp.receive_count).to eq(3)
      end

      before do
        TestApp.receive_count = 0
      end

      it 'should send the request after 2 seconds' do
        7.times { subject.multi_receive([event]) }
        expect(TestApp.receive_count).to eq(0)
        sleep(3)
        expect(TestApp.receive_count).to eq(1)
      end
    end

    context "with retryable failing requests" do
      let(:url) { "http://localhost:#{port}/retry" }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 1, "flush_interval_sec" => 100} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      before do
        subject.register
      end

      before do
        TestApp.retry_fail_count = 5
        event.set("@metadata", {"beat" => "filebeat"})
        event.set("host", {"name" => "LaputadeMacBook-Pro.local"})
        event.set("log", {"file" => {"path" => "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/a"}})
        subject.multi_receive([event])
      end

      it "should log a retryable response 6 times" do
        expect(TestApp.retry_action_count).to eq(6)
        expect(TestApp.final_success).to eq(true)
        event_obj = parse_data(TestApp.last_request.body.read)[0]
        expect(event_obj["event"]).to eq("ViewProduct")
        expect(event_obj["distinct_id"]).to eq("123456")
        expect(event_obj["lib"]["$lib_detail"])
            .to eq("LaputadeMacBook-Pro.local##/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/a")
      end
    end

    context 'config project' do
      let(:url) { "http://localhost:#{port}/good" }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 1, "project" => "production"} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      before do
        event.set("@metadata", {"beat" => "filebeat"})
        event.set("host", {"name" => "LaputadeMacBook-Pro.local"})
        event.set("log", {"file" => {"path" => "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/a"}})
        subject.register
        subject.multi_receive([event])
      end

      it 'should add project into record' do
        event_obj = parse_data(TestApp.last_request.body.read)[0]
        expect(event_obj["project"]).to eq("production")
      end
    end

    context 'config multiple url' do
      let(:url) { Array["http://localhost:#{port}/mult_0",
                        "http://localhost:#{port}/mult_1",
                        "http://localhost:#{port}/mult_2"] }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 1} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      # filebeat input 时默认通过 hostname + path 做 hash
      before do
        TestApp.mult_0 = 0
        TestApp.mult_1 = 0
        TestApp.mult_1 = 0
        TestApp.receive_count = 0
        subject.register
        event.set("@metadata", {"beat" => "filebeat"})
        event.set("host", {"name" => "LaputadeMacBook-Pro.local"})
        (0...100).each { |i|
          event.set("log", {"file" => {"path" => "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/#{i}"}})
          subject.multi_receive([event])
        }
      end

      it 'should send event to multiple url' do
        expect(TestApp.mult_0 > 10).to eq(true)
        expect(TestApp.mult_1 > 10).to eq(true)
        expect(TestApp.mult_2 > 10).to eq(true)
        expect(TestApp.receive_count).to eq(100)
      end
    end

    context 'config multiple url include bad url' do
      let(:url) { Array["http://localhost:#{port}/mult_0",
                        "http://localhost:#{port}/mult_1",
                        "http://localhost:#{port}/mult_2",
                        "http://localhost:#{port}/bad"] }
      let(:verb_behavior_config) { {"url" => url, "flush_batch_size" => 1} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      # filebeat input 时默认通过 hostname + path 做 hash
      before do
        TestApp.mult_0 = 0
        TestApp.mult_1 = 0
        TestApp.mult_1 = 0
        TestApp.receive_count = 0
        subject.register
        event.set("@metadata", {"beat" => "filebeat"})
        event.set("host", {"name" => "LaputadeMacBook-Pro.local"})
        (0...100).each { |i|
          event.set("log", {"file" => {"path" => "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/#{i}"}})
          subject.multi_receive([event])
        }
        subject.close
      end

      it 'should send event to multiple url exclude bad url' do
        expect(TestApp.mult_0 > 15).to eq(true)
        expect(TestApp.mult_1 > 15).to eq(true)
        expect(TestApp.mult_2 > 15).to eq(true)
        expect(TestApp.receive_count).to eq(100)
      end
    end

    context 'config hash filed and send to multiple url include bad url' do
      let(:url) { Array["http://localhost:#{port}/mult_0",
                        "http://localhost:#{port}/mult_1",
                        "http://localhost:#{port}/mult_2",
                        "http://localhost:#{port}/bad"] }
      let(:verb_behavior_config) { {"url" => url,
                                    "flush_batch_size" => 1,
                                    "hash_filed" => Array["name", "[@metadata][path]"]} }
      subject { LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config) }

      # filebeat input 时默认通过 hostname + path 做 hash
      before do
        TestApp.mult_0 = 0
        TestApp.mult_1 = 0
        TestApp.mult_1 = 0
        TestApp.receive_count = 0
        subject.register
        (0...100).each { |i|
          event.set("name", "name#{i}")
          event.set("@metadata", {"path" => "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/#{i}"})
          subject.multi_receive([event])
        }
      end

      it 'should send event to multiple url exclude bad url' do
        expect(TestApp.mult_0 > 10).to eq(true)
        expect(TestApp.mult_1 > 10).to eq(true)
        expect(TestApp.mult_2 > 10).to eq(true)
        expect(TestApp.receive_count).to eq(100)
      end
    end
  end
end
