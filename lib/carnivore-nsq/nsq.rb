require 'krakow'
require 'multi_json'

module Carnivore
  class Source
    class Nsq < Source

      trap_exit :consumer_failure

      DEFAULT_LOOKUPD_PATH = '/etc/carnivore/nsq.json'

      attr_reader(
        :lookupd, :http_transmit, :reader,
        :writer, :topic, :channel, :reader_args,
        :waiter, :producer_args, :args
      )

      def setup(args={})
        @args = args.to_smash
        @lookupd = (default_lookupds + [args[:lookupd]]).flatten.compact.uniq
        @http_transmit = args[:http_transmit]
        @producer_args = args[:producer]
        @topic = args[:topic]
        @channel = args[:channel] || 'default'
        @reader_args = args[:reader_opts] || Smash.new
        @waiter = Celluloid::Condition.new
        Krakow::Utils::Logging.level = (Carnivore::Config.get(:krakow, :logging, :level) || :info).to_sym
      end

      def consumer_failure(*_)
        exclusive do
          warn 'Consumer failure detected. Forcing termination and rebuilding.'
          @reader.terminate
          @reader = nil
          build_consumer
          info "Consumer connection for #{topic}:#{channel} re-established #{reader}"
        end
      end

      def build_consumer
        consumer_args = Smash.new(
          :nsqlookupd => lookupd,
          :topic => topic,
          :channel => channel,
          :max_in_flight => args.fetch(:max_in_flight, 100),
          :notifier => waiter
        ).merge(reader_args)
        @reader = Krakow::Consumer.new(consumer_args)
        link @reader
      end

      def connect
        unless(callbacks.empty?)
          unless(lookupd.empty?)
            build_consumer
            info "Consumer connection for #{topic}:#{channel} established #{reader}"
          end
        end
        if(producer_args)
          @writer = Krakow::Producer.new(
            producer_args.merge(
              Smash.new(
                :topic => topic
              )
            )
          )
          info "Producer TCP connection for #{topic} established #{writer}"
        elsif(http_transmit)
          @writer = Krakow::Producer::Http.new(
            Smash.new(
              :endpoint => http_transmit,
              :topic => topic
            )
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
        begin
          consumer.confirm(message[:message])
        rescue Krakow::Error::LookupFailed => e
          error "Failed to confirm payload from source! (#{e})"
        end
      end

      private

      def default_lookupds
        json_path = args.fetch(:lookupd_file_path, DEFAULT_LOOKUPD_PATH)
        lookupds = nil
        if(File.exists?(json_path))
          begin
            lookupds = MultiJson.load(
              File.read(json_path)
            ).to_smash[:lookupds]
          rescue MultiJson::LoadError => e
            error "Failed to load nsqlookupd file from system: #{e}"
          end
        end
        lookupds || []
      end

    end
  end
end
