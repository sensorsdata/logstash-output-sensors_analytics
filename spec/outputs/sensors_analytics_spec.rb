# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/sensors_analytics"
require "logstash/codecs/plain"
require "logstash/event"

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

  describe "basic test" do

    let(:port) {PORT}
    let(:event) {
      event = LogStash::Event.new
      event.set("path", "/Users/fengjiajie/tools/logstash/logstash-7.1.1/data2/a")
      event.set("host", "LaputadeMacBook-Pro.local")
      event.set("message", '{"distinct_id":"123456","time":1434556935000,"type":"track","event":"ViewProduct","properties":{"product_id":12345,"product_name":"苹果","product_classify":"水果","product_price":14}}')
      event
    }
    let(:method) {"post"}

    context 'sending no events' do
      let(:url) {"http://localhost:#{port}/good"}
      let(:verb_behavior_config) {{"url" => url, "flush_batch_size" => 1}}
      subject {LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config)}

      before do
        subject.register
      end

      it 'should not block the pipeline' do
        subject.multi_receive([])
      end
    end

    context "with retryable failing requests" do
      let(:url) {"http://localhost:#{port}/retry"}
      let(:verb_behavior_config) {{"url" => url, "flush_batch_size" => 1, "flush_interval" => 100}}
      subject {LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config)}

      before do
        subject.register
      end

      before do
        TestApp.retry_fail_count = 3
        subject.multi_receive([event])
      end

      it "should log a retryable response 6 times" do
        expect(TestApp.final_success).to eq(true)
        expect(TestApp.retry_action_count).to eq(4)
        expect(TestApp.last_request.body.read).to eq('data_list=H4sIAAAAAAAA%2F1WPQU7DMBBF7zJlmcZJ2xSRJWuQ2MAGoWqwp%2B20bhzZ06Kqygm4AuIMLNhwoYpjYEe0iJX93x%2F9P%2FN4AMNBuNEyYwM1lKPxpJpCBsIbgrqcRFlNr8ZVURQR7tsIQTzqdZyhHTUS9QPTy513Zqsl0ta7lrwwBagPSSXep%2Ffh2Rk1mCrg%2B%2FXr%2BP4Gf1xbDIHn%2B%2BgdPz7%2Fe61n3e%2FVZWD5OTVc9C%2FcuEUQDMs4nMhsRz6wa6JT5GVenvCGZOnSpeKcDSdqSJBtSsF2K2joFvW1c%2BthPCu3TqMdDNR9iJFqTs1ixbhiUn2Esr%2FF58%2FwMhUqg4IjhdB1Tz8MgoViZwEAAA%3D%3D&gzip=1')
      end
    end

    context 'config project' do
      let(:url) {"http://localhost:#{port}/good"}
      let(:verb_behavior_config) {{"url" => url, "flush_batch_size" => 1, "project" => "production"}}
      subject {LogStash::Outputs::SensorsAnalytics.new(verb_behavior_config)}

      before do
        subject.register
        subject.multi_receive([event])
      end

      it 'should add project into record' do
        expect(TestApp.last_request.body.read).to eq('data_list=H4sIAAAAAAAA%2F1WQTU7DQAyF7%2BKyTDNJ2xSRJWuQ2MAGocpM3NbpNBNl3KKqygm4AuIMLNhwoYpjMDPqj1jN%2BLP13rOf91CxE260zLiCEvLReFJMIQHhNUGZT3xZTG%2FGRZZlHu5aD0E61Cs%2FQ1tqxNdPTG8Pna02WjxtO9tSJ0wOyn2oAo%2FqUTw5owaDBfy%2B%2Fxw%2BP%2BDCtUHneL7zvcPX9%2F9e27GOufoEDL8Gh6v4wp1dOEG39MOBzLbUObaN72RpnuYnvCZZ2rCpWGvciVYkyCaoYLsRrOge9a21q6FfKzVWoxkM1KPzkmpOzaJmrJlUlFDmaHz%2BDK%2BDoapQcKQQ%2Bpi%2BJh1OddwjBOtf%2FgAJu5tJfgEAAA%3D%3D&gzip=1')
      end
    end
  end
end
