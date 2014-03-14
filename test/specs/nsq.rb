require 'minitest/autorun'
require 'carnivore-nsq'

describe 'Carnivore::Source::Nsq' do

  before do
    MessageStore.init
    Carnivore::Source.build(
      :type => :nsq,
      :args => {
        :name => :nsq_source,
        :lookupd => ENV.fetch('NSQ_LOOKUPD', 'http://127.0.0.1:4161'),
        :producer => {
          :host => ENV.fetch('NSQ_PRODUCER_HOST', '127.0.0.1'),
          :port => ENV.fetch('NSQ_PRODUCER_PORT', '4150')
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
  end

  describe 'NSQ interactions' do

    describe 'Building an NSQ based source' do
      it 'returns the source' do
        Carnivore::Supervisor.supervisor[:nsq_source].class.must_be_same_as Carnivore::Source::Nsq
      end
    end

    describe 'NSQ source based communication' do

      describe 'message transmissions' do

        it 'should accept message transmits' do
          Carnivore::Supervisor.supervisor[:nsq_source].transmit('test message')
        end

        it 'should receive messages' do
          Carnivore::Supervisor.supervisor[:nsq_source].transmit('test message 2')
          source_wait
          MessageStore.messages.must_include 'test message 2'
        end

      end
    end

  end
end
