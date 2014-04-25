require 'minitest/autorun'
require 'carnivore-nsq'

describe 'Carnivore::Source::Nsq' do

  before do
    @cluster = NsqCluster.new(:nsqd_count => 1, :nsqlookupd_count => 1)
    sleep(0.4)
    nsqd = @cluster.nsqd.first
    MessageStore.init
    Carnivore::Source.build(
      :type => :nsq,
      :args => {
        :name => :nsq_source,
        :lookupd => @cluster.nsqlookupd_http_endpoints,
        :producer => {
          :host => nsqd.host,
          :port => nsqd.tcp_port
        },
        :topic => 'carnivore-nsq'
      }
    ).add_callback(:store) do |message|
      MessageStore.messages.push(message[:message].message)
      message.confirm!
    end
    @runner = Thread.new{ Carnivore.start! }
    source_wait
  end

  after do
    @runner.terminate if @runner && @runner.alive?
#    @cluster.destroy
  end

  it 'returns the source' do
    Carnivore::Supervisor.supervisor[:nsq_source].class.must_be_same_as Carnivore::Source::Nsq
  end

  it 'should accept message transmits' do
    Carnivore::Supervisor.supervisor[:nsq_source].transmit('test message')
  end

  it 'should receive messages' do
    Carnivore::Supervisor.supervisor[:nsq_source].transmit('test message 2')
    source_wait(3) do
      MessageStore.messages.include?('test message 2')
    end
    MessageStore.messages.must_include 'test message 2'
  end

end
