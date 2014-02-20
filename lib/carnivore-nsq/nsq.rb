require 'krakow'
require 'multi_json'

module Carnivore
  class Source
    class Nsq < Source

      attr_reader(
        :lookupd, :http_transmit, :reader,
        :writer, :topic, :channel, :reader_args
      )

      def setup(args={})
        @lookupd = args[:lookupd]
        @http_transmit = args[:http_transmit]
        @topic = args[:topic]
        @channel = args[:channel] || 'default'
        @reader_args = args[:reader_opts]
      end

      def connect
        if(lookupd)
          consumer_args = {
            :nsqlookupd => lookupd,
            :topic => topic,
            :channel => channel,
            :max_in_flight => 1
          }.merge(reader_args)
          @reader = Krakow::Consumer.new(consumer_args)
          info "Reader connection for #{topic}:#{channel} established #{reader}"
        end
        if(http_transmit)
          @writer = Krakow::Producer::Http.new(
            :endpoint => http_transmit,
            :topic => topic
          )
          info "Producer connection for #{topic} established #{writer}"
        end
      end

      def consumer
        reader ||
          abort('Consumer is not established. No setup information provided!')
      end

      def producer
        reader || writer ||
          abort('Producer is not established. No setup information provided!')
      end

      def receive(n=1)
        msg = consumer.queue.pop
        begin
          MultiJson.load(msg)
        rescue MultiJson::LoadError
          msg
        end
      end

      def transmit(payload, original=nil)
        producer.write(payload)
      end

      def confirm(message)
        consumer.confirm(message)
      end

    end
  end
end
