require 'krakow'
require 'multi_json'

module Carnivore
  class Source
    class Nsq < Source

      attr_reader(
        :lookupd, :http_transmit, :reader,
        :writer, :topic, :channel, :reader_args,
        :waiter, :producer_args
      )

      def setup(args={})
        @lookupd = args[:lookupd]
        @http_transmit = args[:http_transmit]
        @producer_args = args[:producer]
        @topic = args[:topic]
        @channel = args[:channel] || 'default'
        @reader_args = args[:reader_opts] || {}
        @waiter = Celluloid::Condition.new
        Krakow::Utils::Logging.level = (Carnivore::Config.get(:krakow, :logging, :level) || :info).to_sym
      end

      def connect
        if(lookupd)
          consumer_args = {
            :nsqlookupd => lookupd,
            :topic => topic,
            :channel => channel,
            :max_in_flight => 1,
            :notifier => waiter
          }.merge(reader_args)
          @reader = Krakow::Consumer.new(consumer_args)
          info "Reader connection for #{topic}:#{channel} established #{reader}"
        end
        if(producer_args)
          @writer = Krakow::Producer.new(producer_args.merge(:topic => topic))
          info "Producer TCP connection for #{topic} established #{writer}"
        elsif(http_transmit)
          @writer = Krakow::Producer::Http.new(
            :endpoint => http_transmit,
            :topic => topic
          )
          info "Producer HTTP connection for #{topic} established #{writer}"
        end
      end

      def consumer
        reader ||
          abort('Consumer is not established. No setup information provided!')
      end

      def producer
        writer ||
          abort('Producer is not established. No setup information provided!')
      end

      def receive(n=1)
        if(consumer.queue.empty?)
          waiter.wait
        end
        msg = consumer.queue.pop
      end

      def transmit(payload, original=nil)
        payload = MultiJson.dump(payload) unless payload.is_a?(String)
        producer.write(payload)
      end

      def confirm(message)
        consumer.confirm(message[:message])
      end

    end
  end
end
